/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2019      The Fluent Bit Authors
 *  Copyright (C) 2015-2018 Treasure Data Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include <fluent-bit/flb_info.h>
#include <fluent-bit/flb_input.h>
#include <fluent-bit/flb_config.h>
#include <fluent-bit/flb_error.h>
#include <fluent-bit/flb_utils.h>
#include <fluent-bit/flb_pack.h>
#include <fluent-bit/flb_parser.h>
#include <msgpack.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "in_dbus.h"

/* cb_collect callback */
static int in_dbus_collect(struct flb_input_instance *i_ins,
                           struct flb_config *config, void *in_context)
{
    struct flb_in_dbus_config *dbus_config = in_context;
    msgpack_sbuffer* mp_sbuf;

    pthread_mutex_lock(&dbus_config->mut);
    // If there's no data available, then return right away
    if (dbus_config->mp_sbuf == NULL) {
        pthread_mutex_unlock(&dbus_config->mut);
        return 0;
    }

    // Steal the messagepack buffer then release the lock
    mp_sbuf = dbus_config->mp_sbuf;
    dbus_config->mp_sbuf = NULL;
    pthread_mutex_unlock(&dbus_config->mut);

    // Write the data from the buffer, then free it
    flb_input_chunk_append_raw(i_ins, NULL, 0,
                               mp_sbuf->data, mp_sbuf->size);
    msgpack_sbuffer_free(mp_sbuf);

    return 0;
}

static void in_dbus_write_one(struct flb_in_dbus_config *dbus_config)
{
    pthread_mutex_lock(&dbus_config->mut);
    if (dbus_config->mp_sbuf == NULL) {
        dbus_config->mp_sbuf = msgpack_sbuffer_new();
        msgpack_packer_init(&dbus_config->mp_pck, dbus_config->mp_sbuf,
                            msgpack_sbuffer_write);
    }

    struct flb_time out_time;
    flb_time_get(&out_time);

    msgpack_pack_array(&dbus_config->mp_pck, 2);
        flb_time_append_to_msgpack(&out_time, &dbus_config->mp_pck, 0);
        msgpack_pack_map(&dbus_config->mp_pck, 1);
            msgpack_pack_str(&dbus_config->mp_pck, 4);
                msgpack_pack_str_body(&dbus_config->mp_pck, "dbus", 4);
            msgpack_pack_str(&dbus_config->mp_pck, 3);
                msgpack_pack_str_body(&dbus_config->mp_pck, "lol", 3);
    msgpack_pack_array(&dbus_config->mp_pck, 2);
        flb_time_append_to_msgpack(&out_time, &dbus_config->mp_pck, 0);
        msgpack_pack_map(&dbus_config->mp_pck, 1);
            msgpack_pack_str(&dbus_config->mp_pck, 4);
                msgpack_pack_str_body(&dbus_config->mp_pck, "omg", 4);
            msgpack_pack_str(&dbus_config->mp_pck, 3);
                msgpack_pack_str_body(&dbus_config->mp_pck, "wtf", 3);
    pthread_mutex_unlock(&dbus_config->mut);
}

static void* in_dbus_worker(void *in_context)
{
    struct flb_in_dbus_config *dbus_config = in_context;
    while (true) {
        in_dbus_write_one(dbus_config);
        sleep(1);

        // Check for cancellation
        pthread_mutex_lock(&dbus_config->mut);
        if (dbus_config->done) {
            pthread_mutex_unlock(&dbus_config->mut);
            break;
        }
        pthread_mutex_unlock(&dbus_config->mut);
    }
    return NULL;
}

/* read config file and*/
static int in_dbus_config_read(struct flb_in_dbus_config *dbus_config,
                               struct flb_input_instance *in)
{
    const char *path = NULL;

    /* filepath setting */
    path = flb_input_get_property("path", in);
    if (path == NULL) {
        flb_error("[in_dbus] no input 'path' was given");
        return -1;
    }
    dbus_config->path = path;

    return 0;
}

static void delete_dbus_config(struct flb_in_dbus_config *dbus_config)
{
    if (dbus_config) {
        // Ask for the worker thread to shut down
        pthread_mutex_lock(&dbus_config->mut);
        dbus_config->done = true;
        pthread_mutex_unlock(&dbus_config->mut);

        // Join the worker thread, then clean up
        pthread_join(dbus_config->worker, NULL);
        pthread_mutex_destroy(&dbus_config->mut);
        flb_free(dbus_config);
    }
}

/* Initialize plugin */
static int in_dbus_init(struct flb_input_instance *in,
                        struct flb_config *config, void *data)
{
    struct flb_in_dbus_config *dbus_config = NULL;
    int ret = -1;

    /* Allocate space for the configuration */
    dbus_config = flb_malloc(sizeof(struct flb_in_dbus_config));
    if (dbus_config == NULL) {
        return -1;
    }
    dbus_config->mp_sbuf = NULL;
    dbus_config->done = false;
    pthread_mutex_init(&dbus_config->mut, NULL);

    /* Initialize dbus config */
    ret = in_dbus_config_read(dbus_config, in);
    if (ret < 0) {
        goto init_error;
    }
    flb_input_set_context(in, dbus_config);

    ret = flb_input_set_collector_time(in,
                                       in_dbus_collect,
                                       1, 0, config);
    if (ret < 0) {
        flb_error("could not set collector for dbus input plugin");
        goto init_error;
    }

    if (pthread_create(&config->worker, NULL, in_dbus_worker, dbus_config)) {
        flb_error("could not create worker thread");
        goto init_error;
    }

    return 0;

  init_error:
    delete_dbus_config(dbus_config);
    return -1;
}

static int in_dbus_exit(void *data, struct flb_config *config)
{
    (void) *config;
    struct flb_in_dbus_config *dbus_config = data;

    delete_dbus_config(dbus_config);
    return 0;
}


struct flb_input_plugin in_dbus_plugin = {
    .name         = "dbus",
    .description  = "DBus Input",
    .cb_init      = in_dbus_init,
    .cb_pre_run   = NULL,
    .cb_collect   = in_dbus_collect,
    .cb_flush_buf = NULL,
    .cb_exit      = in_dbus_exit
};
