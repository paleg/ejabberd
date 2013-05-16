%%%----------------------------------------------------------------------
%%% File    : mod_logdb.hrl
%%% Author  : Oleg Palij (mailto,xmpp:o.palij@gmail.com)
%%% Purpose :
%%% Version : trunk
%%% Id      : $Id: mod_logdb.hrl 1273 2009-02-05 18:12:57Z malik $
%%% Url     : http://www.dp.uz.gov.ua/o.palij/mod_logdb/
%%%----------------------------------------------------------------------

-define(logdb_debug, true).

-ifdef(logdb_debug).
-define(MYDEBUG(Format, Args), io:format("D(~p:~p:~p) : "++Format++"~n",
                                       [calendar:local_time(),?MODULE,?LINE]++Args)).
-else.
-define(MYDEBUG(_F,_A),[]).
-endif.

-record(msg,   {timestamp,
                owner_name,
                peer_name, peer_server, peer_resource,
                direction,
                type, subject,
                body}).

-record(user_settings, {owner_name,
                        dolog_default,
                        dolog_list=[],
                        donotlog_list=[]}).

-define(INPUTC(Type, Name, Value),
        ?XA("input", [{"type", Type},
                      {"name", Name},
                      {"value", Value},
                      {"checked", "true"}])).
