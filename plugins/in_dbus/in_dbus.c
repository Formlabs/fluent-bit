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

/* cb_collect callback
 * This callback does very little work, because data is being accumulated
 * into the messagepack buffer by a worker thread. */
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

static void in_dbus_log_data(struct flb_in_dbus_config *dbus_config,
                             DBusMessage* msg, DBusConnection* conn)
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
    DBusConnection* conn;
    DBusMessage* msg;

    const char* iface = "com.fluent.fluentbit";

    DBusError err;
    int ret;

    dbus_error_init(&err);
    conn = dbus_bus_get(dbus_config->dbus_bus, &err);
    if (dbus_error_is_set(&err)) {
        flb_error("DBus connection Error (%s)", err.message);
        dbus_error_free(&err);
    }
    if (NULL == conn) {
        flb_error("DBus error: connection is NULL");
        return NULL;
    }

    /* request our name on the bus and check for errors */
    ret = dbus_bus_request_name(conn, dbus_config->dbus_name,
                                DBUS_NAME_FLAG_REPLACE_EXISTING, &err);
    if (dbus_error_is_set(&err)) {
        flb_error("DBus name error (%s)", err.message);
        dbus_error_free(&err);
    }
    if (DBUS_REQUEST_NAME_REPLY_PRIMARY_OWNER != ret) {
        flb_error("DBus error: not Primary Owner (%d)\n", ret);
        return NULL;
    }

    while (true) {
        /* check for cancellation */
        pthread_mutex_lock(&dbus_config->mut);
        if (dbus_config->done) {
            pthread_mutex_unlock(&dbus_config->mut);
            break;
        }
        pthread_mutex_unlock(&dbus_config->mut);

        /* non blocking read of the next available message */
        dbus_connection_read_write(conn, 100);
        msg = dbus_connection_pop_message(conn);

        // loop again if we haven't got a message
        if (NULL == msg) {
            continue;
        }

        flb_info("Got message");
        if (dbus_message_is_method_call(msg, iface, "LogData")) {
            flb_info("Logging data");
            in_dbus_log_data(dbus_config, msg, conn);
        }

        dbus_message_unref(msg);
    }

    dbus_connection_close(conn);
    return NULL;
}

/* read config file and*/
static int in_dbus_config_read(struct flb_in_dbus_config *dbus_config,
                               struct flb_input_instance *in)
{
    const char *str = NULL;

    str = flb_input_get_property("dbus_name", in);
    if (str == NULL) {
        str = "com.fluent.fluentbit";
        flb_info("[in_dbus] 'dbus_name' not found, using default %s", str);
    }
    dbus_config->dbus_name = str;

    str = flb_input_get_property("dbus_bus", in);
    if (str == NULL) {
        dbus_config->dbus_bus = DBUS_BUS_SYSTEM;
        flb_info("[in_dbus] 'dbus_bus' not found, using system bus");
    } else if (!strcmp(str, "system")) {
        dbus_config->dbus_bus = DBUS_BUS_SYSTEM;
        flb_info("[in_dbus] Using system bus");
    } else if (!strcmp(str, "session")) {
        dbus_config->dbus_bus = DBUS_BUS_SESSION;
        flb_info("[in_dbus] Using session bus");
    } else {
        dbus_config->dbus_bus = DBUS_BUS_SYSTEM;
        flb_info("[in_dbus] Invalid bus %s, using system bus", str);
    }

    return 0;
}

static void delete_dbus_config(struct flb_in_dbus_config *dbus_config)
{
    if (dbus_config) {
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

    /* Fill the entire config with zeros to start */
    memset(dbus_config, 0, sizeof(struct flb_in_dbus_config));
    if (pthread_mutex_init(&dbus_config->mut, NULL)) {
        flb_error("could not create mutex");
        flb_free(dbus_config);
        return -1;
    }

    /* Initialize dbus config */
    ret = in_dbus_config_read(dbus_config, in);
    if (ret < 0) {
        delete_dbus_config(dbus_config);
        return -1;
    }
    flb_input_set_context(in, dbus_config);


    /* Start the worker thread running */
    if (pthread_create(&config->worker, NULL, in_dbus_worker, dbus_config)) {
        flb_error("could not create worker thread");
        delete_dbus_config(dbus_config);
        return -1;
    }

    /*  Start the collector running */
    ret = flb_input_set_collector_time(in,
                                       in_dbus_collect,
                                       1, 0, config);
    if (ret < 0) {
        flb_error("could not set collector for dbus input plugin");
        delete_dbus_config(dbus_config);
        return -1;
    }

    return 0;
}

static int in_dbus_exit(void *data, struct flb_config *config)
{
    (void) *config;
    struct flb_in_dbus_config *dbus_config = data;

    // Ask for the worker thread to shut down
    pthread_mutex_lock(&dbus_config->mut);
    dbus_config->done = true;
    pthread_mutex_unlock(&dbus_config->mut);

    // Join the worker thread, then clean up
    pthread_join(dbus_config->worker, NULL);

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
