#!/usr/bin/env bash
# load all modules without external dependencies with postgres

# Copyright (C) 2007 1&1 Internet AG
#
# This file is part of opensips, a free SIP server.
#
# opensips is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version
#
# opensips is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

source include/common
source include/require

# Needs a default opensips database setup for postgres

if ! (check_opensips && check_module "db_postgres" ); then
	exit 0
fi ;

CFG=2.cfg
cp $CFG $CFG.bak

echo "loadmodule \"db_postgres/db_postgres.so\"" >> $CFG
echo "modparam(\"$DB_ALL_MOD\", \"db_url\", \"postgres://opensipsro:opensipsro@localhost/opensips\")" >> $CFG

# start
../opensips -w . -f $CFG > /dev/null
ret=$?

sleep 1
killall -9 opensips

mv $CFG.bak $CFG
rm -f dispatcher.list

exit $ret
