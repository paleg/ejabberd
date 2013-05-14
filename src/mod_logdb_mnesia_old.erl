%%%----------------------------------------------------------------------
%%% File    : mod_logdb_mnesia_old.erl
%%% Author  : Oleg Palij (mailto,xmpp:o.palij@gmail.com)
%%% Purpose : mod_logmnesia backend for mod_logdb (should be used only for copy_tables functionality)
%%% Version : trunk
%%% Id      : $Id: mod_logdb_mnesia_old.erl 1273 2009-02-05 18:12:57Z malik $
%%% Url     : http://www.dp.uz.gov.ua/o.palij/mod_logdb/
%%%----------------------------------------------------------------------

-module(mod_logdb_mnesia_old).
-author('o.palij@gmail.com').

-include("ejabberd.hrl").
-include("jlib.hrl").

-behaviour(gen_logdb).

-export([start/2, stop/1,
         log_message/2,
         rebuild_stats/1,
         rebuild_stats_at/2,
         rebuild_stats_at1/2,
         delete_messages_by_user_at/3, delete_all_messages_by_user_at/3, delete_messages_at/2,
         get_vhost_stats/1, get_vhost_stats_at/2, get_user_stats/2, get_user_messages_at/3,
         get_dates/1,
         get_users_settings/1, get_user_settings/2, set_user_settings/3,
         drop_user/2]).

-record(stats, {user, server, table, count}).
-record(msg,   {to_user, to_server, to_resource, from_user, from_server, from_resource, id, type, subject, body, timestamp}).

tables_prefix() -> "messages_".
% stats_table should not start with tables_prefix(VHost) ! 
% i.e. lists:prefix(tables_prefix(VHost), atom_to_list(stats_table())) must be /= true
stats_table() -> list_to_atom("messages-stats").
% table name as atom from Date
-define(ATABLE(Date), list_to_atom(tables_prefix() ++ Date)).
-define(LTABLE(Date), tables_prefix() ++ Date).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% gen_logdb callbacks
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start(_Opts, _VHost) ->
   case mnesia:system_info(is_running) of
        yes ->
          ok = create_stats_table(),
          {ok, ok};
        no ->
          ?ERROR_MSG("Mnesia not running", []),
          error;
        Status ->
          ?ERROR_MSG("Mnesia status: ~p", [Status]),
          error
   end.

stop(_VHost) ->
   ok.

log_message(_VHost, _Msg) ->
   error.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% gen_logdb callbacks (maintaince)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
rebuild_stats(_VHost) ->
     ok.

rebuild_stats_at(VHost, Date) ->
    Table = ?LTABLE(Date),
    {Time, Value}=timer:tc(?MODULE, rebuild_stats_at1, [VHost, Table]),
    ?INFO_MSG("rebuild_stats_at ~p elapsed ~p sec: ~p~n", [Date, Time/1000000, Value]),
    Value.
rebuild_stats_at1(VHost, Table) ->
    CFun = fun(Msg, Stats) ->
               To = Msg#msg.to_user ++ "@" ++ Msg#msg.to_server,
               Stats_to = if 
                            Msg#msg.to_server == VHost ->
                               case lists:keysearch(To, 1, Stats) of
                                    {value, {Who_to, Count_to}} ->
                                       lists:keyreplace(To, 1, Stats, {Who_to, Count_to + 1});
                                    false ->
                                        lists:append(Stats, [{To, 1}])
                               end;
                            true ->
                               Stats
                          end,
               From = Msg#msg.from_user ++ "@" ++ Msg#msg.from_server,
               Stats_from = if
                              Msg#msg.from_server == VHost  ->
                                 case lists:keysearch(From, 1, Stats_to) of
                                      {value, {Who_from, Count_from}} ->
                                         lists:keyreplace(From, 1, Stats_to, {Who_from, Count_from + 1});
                                      false ->
                                         lists:append(Stats_to, [{From, 1}])
                                 end;
                              true ->
                                 Stats_to
                            end,
               Stats_from
           end,
    DFun = fun(#stats{table=STable, server=Server} = Stat, _Acc)
                when STable == Table, Server == VHost ->
                 mnesia:delete_object(stats_table(), Stat, write);
              (_Stat, _Acc) -> ok
           end,
    case mnesia:transaction(fun() ->
                               mnesia:write_lock_table(list_to_atom(Table)),
                               mnesia:write_lock_table(stats_table()),
                               % Calc stats for VHost at Date
                               AStats = mnesia:foldl(CFun, [], list_to_atom(Table)),
                               % Delete all stats for VHost at Date
                               mnesia:foldl(DFun, [], stats_table()),
                               % Write new calc'ed stats
                               lists:foreach(fun({Who, Count}) ->
                                                 Jid = jlib:string_to_jid(Who),
                                                 JUser = Jid#jid.user,
                                                 WStat = #stats{user=JUser, server=VHost, table=Table, count=Count},
                                                 mnesia:write(stats_table(), WStat, write)
                                             end, AStats)
                            end) of
         {aborted, Reason} ->
              ?ERROR_MSG("Failed to rebuild_stats_at for ~p at ~p: ~p", [VHost, Table, Reason]),
              error;
         {atomic, _} ->
              ok
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% gen_logdb callbacks (delete)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
delete_messages_by_user_at(_VHost, _Msgs, _Date) ->
    error.

delete_all_messages_by_user_at(_User, _VHost, _Date) ->
    error.

delete_messages_at(VHost, Date) ->
   Table = list_to_atom(tables_prefix() ++ Date),

   DFun = fun(#msg{to_server=To_server, from_server=From_server}=Msg, _Acc)
                when To_server == VHost; From_server == VHost ->
                   mnesia:delete_object(Table, Msg, write);
             (_Msg, _Acc) -> ok
          end,
   
   case mnesia:transaction(fun() ->
                            mnesia:foldl(DFun, [], Table)
                           end) of
        {aborted, Reason} ->
            ?ERROR_MSG("Failed to delete_messages_at for ~p at ~p: ~p", [VHost, Date, Reason]),
            error;
        {atomic, _} ->
            ok
   end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% gen_logdb callbacks (get)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_vhost_stats(_VHost) ->
    {error, "does not emplemented"}.

get_vhost_stats_at(VHost, Date) ->
    Fun = fun() ->
             Pat = #stats{user='$1', server=VHost, table=tables_prefix()++Date, count = '$2'},
             mnesia:select(stats_table(), [{Pat, [], [['$1', '$2']]}])
          end,
    case mnesia:transaction(Fun) of
         {atomic, Result} ->
                   RFun = fun([User, Count]) ->
                             {User, Count}
                          end,
                   {ok, lists:reverse(lists:keysort(2, lists:map(RFun, Result)))};
         {aborted, Reason} -> {error, Reason}
    end.

get_user_stats(_User, _VHost) ->
    {error, "does not emplemented"}.

get_user_messages_at(User, VHost, Date) ->
    Table_name = tables_prefix() ++ Date,
    case mnesia:transaction(fun() ->
                               Pat_to = #msg{to_user=User, to_server=VHost, _='_'},
                               Pat_from = #msg{from_user=User, from_server=VHost,  _='_'},
                               mnesia:select(list_to_atom(Table_name),
                                             [{Pat_to, [], ['$_']},
                                              {Pat_from, [], ['$_']}])
                       end) of
          {atomic, Result} ->
                   Msgs = lists:map(fun(#msg{to_user=To_user, to_server=To_server, to_resource=To_res,
                                             from_user=From_user, from_server=From_server, from_resource=From_res,
                                             type=Type,
                                             subject=Subj,
                                             body=Body, timestamp=Timestamp} = _Msg) ->
                                        Subject = case Subj of
                                                       "None" -> "";
                                                       _ -> Subj
                                                  end,
                                        {msg, To_user, To_server, To_res, From_user, From_server, From_res, Type, Subject, Body, Timestamp}
                                    end, Result),
                   {ok, Msgs};
          {aborted, Reason} ->
                   {error, Reason}
    end.

get_dates(_VHost) ->
    Tables = mnesia:system_info(tables),
    MessagesTables =
        lists:filter(fun(Table) ->
                         lists:prefix(tables_prefix(), atom_to_list(Table))
                     end,
                     Tables),
    lists:map(fun(Table) ->
                  lists:sublist(atom_to_list(Table),
                                length(tables_prefix())+1,
                                length(atom_to_list(Table)))
              end,
              MessagesTables).

get_users_settings(_VHost) ->
    {ok, []}.
get_user_settings(_User, _VHost) ->
    {ok, []}.
set_user_settings(_User, _VHost, _Set) ->
    ok.
drop_user(_User, _VHost) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% internal 
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% called from db_logon/2
create_stats_table() ->
    SName = stats_table(),
    case mnesia:create_table(SName,
                             [{disc_only_copies, [node()]},
                              {type, bag},
                              {attributes, record_info(fields, stats)},
                              {record_name, stats}
                             ]) of
         {atomic, ok} ->
             ?INFO_MSG("Created stats table", []),
             ok;
         {aborted, {already_exists, _}} ->
             ok;
         {aborted, Reason} ->
             ?ERROR_MSG("Failed to create stats table: ~p", [Reason]),
             error
    end.
