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
#include <dbus/dbus.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "in_dbus.h"

struct in_dbus_event {
    struct mk_event event;
    struct mk_event_loop* evl;
    DBusConnection* conn;
    DBusWatch* watch;
};

int in_dbus_event_handler(void* data) {
    struct in_dbus_event* event = data;
    const unsigned flags = dbus_watch_get_flags(event->watch);
    if (!dbus_watch_handle(event->watch, flags)) {
        flb_error("[in_dbus] dbus_watch_handle failed");
    }

    while (dbus_connection_get_dispatch_status(event->conn) ==
           DBUS_DISPATCH_DATA_REMAINS) {
        dbus_connection_dispatch(event->conn);
    }
    return 0;
}

dbus_bool_t in_dbus_add_watch(DBusWatch* watch, void* data)
{
    struct in_dbus_event* event = data;
    const unsigned flags = dbus_watch_get_flags(watch);
    const int fd = dbus_watch_get_unix_fd(watch);
    unsigned mk_flags = 0;
    if (flags & DBUS_WATCH_READABLE) {
        mk_flags |= MK_EVENT_READ;
    }
    if (flags & DBUS_WATCH_WRITABLE) {
        mk_flags |= MK_EVENT_WRITE;
    }
    event->watch = watch;
    mk_event_add(event->evl, fd, FLB_ENGINE_EV_CUSTOM, mk_flags, data);
    return true;
}

void in_dbus_remove_watch(DBusWatch* watch, void* data)
{
    struct in_dbus_event* event = data;
    mk_event_del(event->evl, data);
}

/* cb_collect callback
 * This callback does very little work, because data is being accumulated
 * into the messagepack buffer by a separate task. */
static int in_dbus_collect(struct flb_input_instance *i_ins,
                           struct flb_config *config, void *in_context)
{
    struct flb_in_dbus_config *dbus_config = in_context;
    if (dbus_config->mp_sbuf.size != 0) {
        flb_input_chunk_append_raw(i_ins, NULL, 0,
                                   dbus_config->mp_sbuf.data,
                                   dbus_config->mp_sbuf.size);
        msgpack_sbuffer_clear(&dbus_config->mp_sbuf);
    }

    return 0;
}

static void in_dbus_reply_error(DBusMessage* msg, DBusConnection* conn,
                                const char* message)
{
    DBusMessage* reply;
    dbus_uint32_t serial = 0;

    reply = dbus_message_new_error(msg, DBUS_ERROR_FAILED, message);
    if (dbus_connection_send(conn, reply, &serial)) {
        dbus_connection_flush(conn);
    }
    else {
        flb_error("[in_dbus] Failed to send error reply");
    }
    dbus_message_unref(reply);
}

static void in_dbus_introspect(struct flb_in_dbus_config *dbus_config,
                               DBusMessage* msg, DBusConnection* conn)
{
    DBusMessage* reply;
    DBusMessageIter args;
    dbus_uint32_t serial = 0;
    const char* description = ""
        "<!DOCTYPE node PUBLIC \"-//freedesktop//DTD D-BUS Object Introspection 1.0//EN\""
        "        \"http://www.freedesktop.org/standards/dbus/1.0/introspect.dtd\">"
        "<node>"
        "    <interface name=\"com.fluent.fluentbit\">"
        "        <method name=\"LogData\">"
        "            <arg name=\"data\" type=\"a{sv}\" direction=\"in\"/>"
        "            <annotation name=\"org.qtproject.QtDBus.QtTypeName.In0\" value=\"QVariantMap\"/>"
        "        </method>"
        "        <method name=\"LogTimestampedData\">"
        "            <arg name=\"tv_sec\"  type=\"x\" direction=\"in\"/>"
        "            <arg name=\"tv_nsec\" type=\"x\" direction=\"in\"/>"
        "            <arg name=\"data\" type=\"a{sv}\" direction=\"in\"/>"
        "            <annotation name=\"org.qtproject.QtDBus.QtTypeName.In2\" value=\"QVariantMap\"/>"
        "        </method>"
        "    </interface>"
        "</node>";

    reply = dbus_message_new_method_return(msg);

    dbus_message_iter_init_append(reply, &args);
    if (!dbus_message_iter_append_basic(
                &args, DBUS_TYPE_STRING, &description)) {
        flb_error("[in_dbus] Could not append description");
    }
    else if (!dbus_connection_send(conn, reply, &serial)) {
        flb_error("[in_dbus] Could not send reply");
    }
    else {
        dbus_connection_flush(conn);
    }
    dbus_message_unref(reply);
}

static void in_dbus_log_dict(struct flb_in_dbus_config *dbus_config,
                             struct flb_time *out_time,
                             DBusMessage *msg,
                             DBusMessageIter *args,
                             DBusConnection *conn)
{
    DBusMessageIter dict;
    DBusMessage* reply;
    dbus_uint32_t serial = 0;

    /*  Make sure that we're at the right part of the message */
    if (DBUS_TYPE_ARRAY != dbus_message_iter_get_arg_type(args)) {
        flb_error("[in_dbus] Message is not a dictionary");
        in_dbus_reply_error(msg, conn, "Method call has invalid arguments");
        return;
    }

    /*  Write the time for this sample */
    msgpack_pack_array(&dbus_config->mp_pck, 2);
    flb_time_append_to_msgpack(out_time, &dbus_config->mp_pck, 0);

    /*  Count up arguments, and record this in the pack */
    unsigned count = 0;
    for (dbus_message_iter_recurse(args, &dict);
         dbus_message_iter_get_arg_type(&dict) != DBUS_TYPE_INVALID;
         dbus_message_iter_next(&dict), count++);
    msgpack_pack_map(&dbus_config->mp_pck, count);

    /*  Iterate over items in the map, packing each one */
    for (dbus_message_iter_recurse(args, &dict);
         dbus_message_iter_get_arg_type(&dict) != DBUS_TYPE_INVALID;
         dbus_message_iter_next(&dict))
    {
        DBusMessageIter entry;
        DBusMessageIter variant;
        char* key = "";

        if (dbus_message_iter_get_arg_type(&dict) != DBUS_TYPE_DICT_ENTRY) {
            flb_error("[in_dbus] Expected dictionary key");
            break;
        }

        dbus_message_iter_recurse(&dict, &entry);
        if (DBUS_TYPE_STRING != dbus_message_iter_get_arg_type(&entry)) {
            flb_error("[in_dbus] Expected string key");
            break;
        }
        dbus_message_iter_get_basic(&entry, &key);

        dbus_message_iter_next(&entry);
        if (DBUS_TYPE_VARIANT != dbus_message_iter_get_arg_type(&entry)) {
            flb_error("[in_dbus] Expected variant key");
            break;
        }

        dbus_message_iter_recurse(&entry, &variant);

        {   /* Pack the key */
            size_t key_len = strlen(key);
            msgpack_pack_str(&dbus_config->mp_pck, key_len);
            msgpack_pack_str_body(&dbus_config->mp_pck, key, key_len);
        }

        switch (dbus_message_iter_get_arg_type(&variant)) {
#define GET(DBUS_TYPE, TYPE, MSGPACK_FUNC)                  \
            case DBUS_TYPE: {                               \
                TYPE v;                                     \
                dbus_message_iter_get_basic(&variant, &v);  \
                MSGPACK_FUNC(&dbus_config->mp_pck, v);      \
                break;                                      \
            }
            GET(DBUS_TYPE_INT16,  int16_t,  msgpack_pack_int16);
            GET(DBUS_TYPE_UINT16, uint16_t, msgpack_pack_uint16);
            GET(DBUS_TYPE_INT32,  int32_t,  msgpack_pack_int32);
            GET(DBUS_TYPE_UINT32, uint32_t, msgpack_pack_uint32);
            GET(DBUS_TYPE_INT64,  int64_t,  msgpack_pack_int64);
            GET(DBUS_TYPE_UINT64, uint64_t, msgpack_pack_uint64);
            GET(DBUS_TYPE_DOUBLE, double,   msgpack_pack_double);
            GET(DBUS_TYPE_BYTE,   uint8_t,  msgpack_pack_uint8);
#undef GET
            case DBUS_TYPE_STRING: {
                char* s;
                size_t len;

                dbus_message_iter_get_basic(&variant, &s);
                len = strlen(s);
                msgpack_pack_str(&dbus_config->mp_pck, len);
                msgpack_pack_str_body(&dbus_config->mp_pck, s, len);
                break;
            }
            case DBUS_TYPE_BOOLEAN: {
                dbus_bool_t b;
                dbus_message_iter_get_basic(&variant, &b);
                if (b) {
                    msgpack_pack_true(&dbus_config->mp_pck);
                }
                else {
                    msgpack_pack_false(&dbus_config->mp_pck);
                }
                break;
            }
            default:
                flb_error("[in_dbus] Unknown type '%c'",
                          dbus_message_iter_get_arg_type(&variant));
                break;
        }
    }

    /*  Send a DBus reply */
    reply = dbus_message_new_method_return(msg);
    if (!dbus_connection_send(conn, reply, &serial)) {
        flb_error("[in_dbus] Could not send reply");
    } else {
        dbus_connection_flush(conn);
    }
    dbus_message_unref(reply);
}

static void in_dbus_log_data(struct flb_in_dbus_config *dbus_config,
                             DBusMessage* msg, DBusConnection* conn)
{
    DBusMessageIter args;

    struct flb_time out_time;
    flb_time_get(&out_time);

    /* read the arguments, returning immediately if they're invalid */
    if (!dbus_message_iter_init(msg, &args)) {
        flb_error("[in_dbus] Message has no arguments");
        in_dbus_reply_error(msg, conn, "Method call has no arguments");
        return;
    }
    in_dbus_log_dict(dbus_config, &out_time, msg, &args, conn);
}

static void in_dbus_log_timestamped_data(
        struct flb_in_dbus_config *dbus_config,
        DBusMessage* msg, DBusConnection* conn)
{
    DBusMessageIter args;
    struct flb_time out_time;
    int64_t x;

    /* read the arguments, returning immediately if they're invalid */
    if (!dbus_message_iter_init(msg, &args)) {
        flb_error("[in_dbus] Message has no arguments");
        in_dbus_reply_error(msg, conn, "Method call has no arguments");
        return;
    }

    if (dbus_message_iter_get_arg_type(&args) != DBUS_TYPE_INT64) {
        flb_error("[in_dbus] Expected int64 for tv_sec");
        in_dbus_reply_error(msg, conn, "Expected int64 for tv_sec");
        return;
    }
    dbus_message_iter_get_basic(&args, &x);
    out_time.tm.tv_sec = x;
    dbus_message_iter_next(&args);

    if (dbus_message_iter_get_arg_type(&args) != DBUS_TYPE_INT64) {
        flb_error("[in_dbus] Expected int64 for tv_nsec");
        in_dbus_reply_error(msg, conn, "Expected int64 for tv_nsec");
        return;
    }
    dbus_message_iter_get_basic(&args, &x);
    out_time.tm.tv_nsec = x;
    dbus_message_iter_next(&args);

    in_dbus_log_dict(dbus_config, &out_time, msg, &args, conn);
}

static DBusHandlerResult in_dbus_vtable_message(DBusConnection *conn, DBusMessage *msg,
                            void* user_data)
{
    const char* iface = "com.fluent.fluentbit";

    struct flb_in_dbus_config *dbus_config = user_data;
    if (dbus_message_is_method_call(
            msg, "org.freedesktop.DBus.Introspectable", "Introspect")) {
        in_dbus_introspect(dbus_config, msg, conn);
        return DBUS_HANDLER_RESULT_HANDLED;
    }
    else if (dbus_message_is_method_call(msg, iface, "LogData")) {
        in_dbus_log_data(dbus_config, msg, conn);
        return DBUS_HANDLER_RESULT_HANDLED;
    }
    else if (dbus_message_is_method_call(msg, iface, "LogTimestampedData")) {
        in_dbus_log_timestamped_data(dbus_config, msg, conn);
        return DBUS_HANDLER_RESULT_HANDLED;
    }
    return DBUS_HANDLER_RESULT_NOT_YET_HANDLED;
}

static DBusObjectPathVTable in_dbus_vtable = (DBusObjectPathVTable){
    .message_function = in_dbus_vtable_message,
};

static bool in_dbus_install(struct flb_config *config,
                            struct flb_in_dbus_config *dbus_config)
{
    DBusError err;
    int ret;
    bool already_running = false;

    dbus_error_init(&err);
    dbus_config->conn = dbus_bus_get(dbus_config->dbus_bus, &err);
    if (dbus_error_is_set(&err)) {
        flb_error("[in_dbus] DBus connection Error (%s)", err.message);
        dbus_error_free(&err);
        return false;
    }
    if (NULL == dbus_config->conn) {
        flb_error("[in_dbus] DBus error: connection is NULL");
        return false;
    }
    dbus_connection_set_exit_on_disconnect(dbus_config->conn, false);

    /* request our name on the bus and check for errors */
    ret = dbus_bus_request_name(dbus_config->conn, "com.fluent.fluentbit",
                                DBUS_NAME_FLAG_REPLACE_EXISTING, &err);
    if (dbus_error_is_set(&err)) {
        flb_error("[in_dbus] DBus name error (%s)", err.message);
        dbus_error_free(&err);
        return false;
    }
    else if (ret == DBUS_REQUEST_NAME_REPLY_ALREADY_OWNER) {
        already_running = true;
    }
    else if (DBUS_REQUEST_NAME_REPLY_PRIMARY_OWNER != ret) {
        flb_error("[in_dbus] DBus error: not Primary Owner (%d)", ret);
        return false;
    }

    if (!dbus_connection_register_object_path(
            dbus_config->conn,
            dbus_config->dbus_object_path,
            &in_dbus_vtable,
            dbus_config)) {
        flb_error("[in_dbus] Could not claim object path %s",
                  dbus_config->dbus_object_path);
        return false;
    }

    if (!already_running) {
        struct in_dbus_event* event;

        event = flb_calloc(1, sizeof(*event));
        event->evl = config->evl;
        event->event.handler = in_dbus_event_handler;
        event->conn = dbus_config->conn;
        dbus_connection_set_watch_functions(dbus_config->conn,
                in_dbus_add_watch,
                in_dbus_remove_watch,
                NULL,
                event,
                NULL);
    }

    return true;
}

/* read config file and*/
static int in_dbus_config_read(struct flb_in_dbus_config *dbus_config,
                               struct flb_input_instance *in)
{
    const char *str = NULL;

    str = flb_input_get_property("dbus_objectpath", in);
    if (str == NULL) {
        flb_error("[in_dbus] Missing DBus_ObjectPath");
        return -1;
    }
    dbus_config->dbus_object_path = str;

    str = flb_input_get_property("dbus_bus", in);
    if (str == NULL) {
        dbus_config->dbus_bus = DBUS_BUS_SYSTEM;
        flb_trace("[in_dbus] 'dbus_bus' not found, using system bus");
    }
    else if (!strcmp(str, "system")) {
        dbus_config->dbus_bus = DBUS_BUS_SYSTEM;
        flb_trace("[in_dbus] Using system bus");
    }
    else if (!strcmp(str, "session")) {
        dbus_config->dbus_bus = DBUS_BUS_SESSION;
        flb_trace("[in_dbus] Using session bus");
    }
    else {
        flb_error("[in_dbus] Invalid bus '%s' "
                  "(must be 'system' or 'session')", str);
        return -1;
    }

    return 0;
}

static void delete_dbus_config(struct flb_in_dbus_config *dbus_config)
{
    if (dbus_config) {
        if (dbus_config->conn) {
            dbus_connection_unregister_object_path(
                dbus_config->conn,
                dbus_config->dbus_object_path);
        }
        msgpack_sbuffer_destroy(&dbus_config->mp_sbuf);
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
    dbus_config = flb_calloc(1, sizeof(struct flb_in_dbus_config));
    if (dbus_config == NULL) {
        return -1;
    }

    /* Set up the messagepack buffer and writer */
    msgpack_sbuffer_init(&dbus_config->mp_sbuf);
    msgpack_packer_init(&dbus_config->mp_pck, &dbus_config->mp_sbuf,
                        msgpack_sbuffer_write);

    /* Initialize dbus config */
    ret = in_dbus_config_read(dbus_config, in);
    if (ret < 0) {
        delete_dbus_config(dbus_config);
        return -1;
    }
    flb_input_set_context(in, dbus_config);

    /* Start the worker thread running */
    if (!in_dbus_install(config, dbus_config)) {
        flb_error("[in_dbus] could not install DBus system");
        delete_dbus_config(dbus_config);
        return -1;
    }

    /*  Start the collector running */
    ret = flb_input_set_collector_time(in,
                                       in_dbus_collect,
                                       1, 0, config);
    if (ret < 0) {
        flb_error("[in_dbus] could not set collector for dbus input plugin");
        delete_dbus_config(dbus_config);
        return -1;
    }

    return 0;
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