/*
Copyright (c) 2012-2018 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
/* system commands */
#include "serv.h"
#include "net/proc.h"
#include "net/server.h"

DEF_PROC(flushdb);
DEF_PROC(info);
DEF_PROC(version);
DEF_PROC(dbsize);
DEF_PROC(compact);
DEF_LINK_PROC(dump);
DEF_LINK_PROC(sync140);
DEF_PROC(clear_binlog);
