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
#ifndef FLB_IN_DBUS_H
#define FLB_IN_DBUS_H

#include <fluent-bit/flb_input.h>
#include <msgpack.h>

struct flb_in_dbus_config {
    /*  Populated during init and never changed */
    const char *dbus_object_path;

    /* DBUS_BUS_[SYSTEM|SESSION] */
    int dbus_bus;

    msgpack_sbuffer* mp_sbuf;

    /*  Everything below here is maintained by the worker */
    msgpack_packer mp_pck;
    DBusConnection* conn;
};

extern struct flb_input_plugin in_dbus_plugin;

#endif /* FLB_IN_DBUS_H */
