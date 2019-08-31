%%%----------------------------------------------------------------------
%%% File    : mod_logdb_mnesia.erl
%%% Author  : Oleg Palij (mailto:o.palij@gmail.com)
%%% Purpose : mnesia backend for mod_logdb
%%% Url     : https://paleg.github.io/mod_logdb/
%%%----------------------------------------------------------------------

-module(mod_logdb_mnesia).
-author('o.palij@gmail.com').

-include("mod_logdb.hrl").
-include("logger.hrl").

-behaviour(gen_logdb).
-behaviour(gen_server).

% gen_server
-export([code_change/3,handle_call/3,handle_cast/2,handle_info/2,init/1,terminate/2]).
% gen_mod
-export([start/2, stop/1]).
% gen_logdb
-export([log_message/2,
         rebuild_stats/1,
         rebuild_stats_at/2,
         delete_messages_by_user_at/3, delete_all_messages_by_user_at/3, delete_messages_at/2,
         get_vhost_stats/1, get_vhost_stats_at/2, get_user_stats/2, get_user_messages_at/3,
         get_dates/1,
         get_users_settings/1, get_user_settings/2, set_user_settings/3,
         drop_user/2]).

-define(PROCNAME, mod_logdb_mnesia).
-define(CALL_TIMEOUT, 10000).

-record(state, {vhost}).

-record(stats, {user, at, count}).

prefix() ->
   "logdb_".

suffix(VHost) ->
   "_" ++ binary_to_list(VHost).

stats_table(VHost) ->
   list_to_atom(prefix() ++ "stats" ++ suffix(VHost)).

table_name(VHost, Date) ->
   list_to_atom(prefix() ++ "messages_" ++ Date ++ suffix(VHost)).

settings_table(VHost) ->
   list_to_atom(prefix() ++ "settings" ++ suffix(VHost)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% gen_mod callbacks
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start(VHost, Opts) ->
   Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
   gen_server:start({local, Proc}, ?MODULE, [VHost, Opts], []).

stop(VHost) ->
   Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
   gen_server:call(Proc, {stop}, ?CALL_TIMEOUT).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% gen_server callbacks
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init([VHost, _Opts]) ->
   case mnesia:system_info(is_running) of
        yes ->
          ok = create_stats_table(VHost),
          ok = create_settings_table(VHost),
          {ok, #state{vhost=VHost}};
        no ->
          ?ERROR_MSG("Mnesia not running", []),
          {stop, db_connection_failed};
        Status ->
          ?ERROR_MSG("Mnesia status: ~p", [Status]),
          {stop, db_connection_failed}
   end.

handle_call({log_message, Msg}, _From, #state{vhost=VHost}=State) ->
    {reply, log_message_int(VHost, Msg), State};
handle_call({rebuild_stats}, _From, #state{vhost=VHost}=State) ->
    {atomic, ok} = delete_nonexistent_stats(VHost),
    Reply =
      lists:foreach(fun(Date) ->
                        rebuild_stats_at_int(VHost, Date)
                    end, get_dates_int(VHost)),
    {reply, Reply, State};
handle_call({rebuild_stats_at, Date}, _From, #state{vhost=VHost}=State) ->
    Reply = rebuild_stats_at_int(VHost, Date),
    {reply, Reply, State};
handle_call({delete_messages_by_user_at, Msgs, Date}, _From, #state{vhost=VHost}=State) ->
    Table = table_name(VHost, Date),
    Fun = fun() ->
             lists:foreach(
                fun(Msg) ->
                    mnesia:write_lock_table(stats_table(VHost)),
                    mnesia:write_lock_table(Table),
                    mnesia:delete_object(Table, Msg, write)
               end, Msgs)
          end,
    DRez = case mnesia:transaction(Fun) of
                {aborted, Reason} ->
                   ?ERROR_MSG("Failed to delete_messages_by_user_at at ~p for ~p: ~p", [Date, VHost, Reason]),
                   error;
                _ ->
                   ok
           end,
    Reply =
      case rebuild_stats_at_int(VHost, Date) of
           error ->
             error;
           ok ->
             DRez
      end,
    {reply, Reply, State};
handle_call({delete_all_messages_by_user_at, User, Date}, _From, #state{vhost=VHost}=State) ->
    {reply, delete_all_messages_by_user_at_int(User, VHost, Date), State};
handle_call({delete_messages_at, Date}, _From, #state{vhost=VHost}=State) ->
    Reply =
      case mnesia:delete_table(table_name(VHost, Date)) of
           {atomic, ok} ->
              delete_stats_by_vhost_at_int(VHost, Date);
           {aborted, Reason} ->
              ?ERROR_MSG("Failed to delete_messages_at for ~p at ~p", [VHost, Date, Reason]),
              error
      end,
    {reply, Reply, State};
handle_call({get_vhost_stats}, _From, #state{vhost=VHost}=State) ->
    Fun = fun(#stats{at=Date, count=Count}, Stats) ->
              case lists:keysearch(Date, 1, Stats) of
                   false ->
                      lists:append(Stats, [{Date, Count}]);
                   {value, {_, TempCount}} ->
                      lists:keyreplace(Date, 1, Stats, {Date, TempCount+Count})
              end
          end,
    Reply =
      case mnesia:transaction(fun() ->
                                   mnesia:foldl(Fun, [], stats_table(VHost))
                                end) of
             {atomic, Result} -> {ok, mod_logdb:sort_stats(Result)};
             {aborted, Reason} -> {error, Reason}
      end,
    {reply, Reply, State};
handle_call({get_vhost_stats_at, Date}, _From, #state{vhost=VHost}=State) ->
    Fun = fun() ->
             Pat = #stats{user='$1', at=Date, count='$2'},
             mnesia:select(stats_table(VHost), [{Pat, [], [['$1', '$2']]}])
          end,
    Reply =
      case mnesia:transaction(Fun) of
           {atomic, Result} ->
                     {ok, lists:reverse(lists:keysort(2, [{User, Count} || [User, Count] <- Result]))};
           {aborted, Reason} ->
                     {error, Reason}
      end,
    {reply, Reply, State};
handle_call({get_user_stats, User}, _From, #state{vhost=VHost}=State) ->
    {reply, get_user_stats_int(User, VHost), State};
handle_call({get_user_messages_at, User, Date}, _From, #state{vhost=VHost}=State) ->
    Reply =
      case mnesia:transaction(fun() ->
                                Pat = #msg{owner_name=User, _='_'},
                                mnesia:select(table_name(VHost, Date),
                                              [{Pat, [], ['$_']}])
                        end) of
           {atomic, Result} -> {ok, Result};
           {aborted, Reason} ->
                    {error, Reason}
      end,
    {reply, Reply, State};
handle_call({get_dates}, _From, #state{vhost=VHost}=State) ->
    {reply, get_dates_int(VHost), State};
handle_call({get_users_settings}, _From, #state{vhost=VHost}=State) ->
    Reply = mnesia:dirty_match_object(settings_table(VHost), #user_settings{_='_'}),
    {reply, {ok, Reply}, State};
handle_call({get_user_settings, User}, _From, #state{vhost=VHost}=State) ->
   Reply =
    case mnesia:dirty_match_object(settings_table(VHost), #user_settings{owner_name=User, _='_'}) of
         [] -> [];
         [Setting] ->
            Setting
    end,
   {reply, Reply, State};
handle_call({set_user_settings, _User, Set}, _From, #state{vhost=VHost}=State) ->
    ?MYDEBUG("~p~n~p", [settings_table(VHost), Set]),
    Reply = mnesia:dirty_write(settings_table(VHost), Set),
    ?MYDEBUG("~p", [Reply]),
    {reply, Reply, State};
handle_call({drop_user, User}, _From, #state{vhost=VHost}=State) ->
    {ok, Dates} = get_user_stats_int(User, VHost),
    MDResult = lists:map(fun({Date, _}) ->
                   delete_all_messages_by_user_at_int(User, VHost, Date)
               end, Dates),
    SDResult = delete_user_settings_int(User, VHost),
    Reply =
      case lists:all(fun(Result) when Result == ok ->
                          true;
                        (Result) when Result == error ->
                          false
                     end, lists:append(MDResult, [SDResult])) of
           true ->
             ok;
           false ->
             error
      end,
    {reply, Reply, State};
handle_call({stop}, _From, State) ->
   {stop, normal, ok, State};
handle_call(Msg, _From, State) ->
    ?INFO_MSG("Got call Msg: ~p, State: ~p", [Msg, State]),
    {noreply, State}.

handle_cast(Msg, State) ->
    ?INFO_MSG("Got cast Msg:~p, State:~p", [Msg, State]),
    {noreply, State}.

handle_info(Info, State) ->
    ?INFO_MSG("Got Info:~p, State:~p", [Info, State]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% gen_logdb callbacks
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
log_message(VHost, Msg) ->
   Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
   gen_server:call(Proc, {log_message, Msg}, ?CALL_TIMEOUT).
rebuild_stats(VHost) ->
   Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
   gen_server:call(Proc, {rebuild_stats}, ?CALL_TIMEOUT).
rebuild_stats_at(VHost, Date) ->
   Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
   gen_server:call(Proc, {rebuild_stats_at, Date}, ?CALL_TIMEOUT).
delete_messages_by_user_at(VHost, Msgs, Date) ->
   Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
   gen_server:call(Proc, {delete_messages_by_user_at, Msgs, Date}, ?CALL_TIMEOUT).
delete_all_messages_by_user_at(User, VHost, Date) ->
   Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
   gen_server:call(Proc, {delete_all_messages_by_user_at, User, Date}, ?CALL_TIMEOUT).
delete_messages_at(VHost, Date) ->
   Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
   gen_server:call(Proc, {delete_messages_at, Date}, ?CALL_TIMEOUT).
get_vhost_stats(VHost) ->
   Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
   gen_server:call(Proc, {get_vhost_stats}, ?CALL_TIMEOUT).
get_vhost_stats_at(VHost, Date) ->
   Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
   gen_server:call(Proc, {get_vhost_stats_at, Date}, ?CALL_TIMEOUT).
get_user_stats(User, VHost) ->
   Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
   gen_server:call(Proc, {get_user_stats, User}, ?CALL_TIMEOUT).
get_user_messages_at(User, VHost, Date) ->
   Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
   gen_server:call(Proc, {get_user_messages_at, User, Date}, ?CALL_TIMEOUT).
get_dates(VHost) ->
   Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
   gen_server:call(Proc, {get_dates}, ?CALL_TIMEOUT).
get_user_settings(User, VHost) ->
   Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
   gen_server:call(Proc, {get_user_settings, User}, ?CALL_TIMEOUT).
get_users_settings(VHost) ->
   Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
   gen_server:call(Proc, {get_users_settings}, ?CALL_TIMEOUT).
set_user_settings(User, VHost, Set) ->
   Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
   gen_server:call(Proc, {set_user_settings, User, Set}, ?CALL_TIMEOUT).
drop_user(User, VHost) ->
   Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
   gen_server:call(Proc, {drop_user, User}, ?CALL_TIMEOUT).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% internals
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
log_message_int(VHost, #msg{timestamp=Timestamp}=MsgBin) ->
    Date = mod_logdb:convert_timestamp_brief(Timestamp),

    Msg = #msg{timestamp     = MsgBin#msg.timestamp,
               owner_name    = binary_to_list(MsgBin#msg.owner_name),
               peer_name     = binary_to_list(MsgBin#msg.peer_name),
               peer_server   = binary_to_list(MsgBin#msg.peer_server),
               peer_resource = binary_to_list(MsgBin#msg.peer_resource),
               direction     = MsgBin#msg.direction,
               type          = binary_to_list(MsgBin#msg.type),
               subject       = binary_to_list(MsgBin#msg.subject),
               body          = binary_to_list(MsgBin#msg.body)},

    ATable = table_name(VHost, Date),
    Fun = fun() ->
              mnesia:write_lock_table(ATable),
              mnesia:write(ATable, Msg, write)
          end,
    % log message, increment stats for both users
    case mnesia:transaction(Fun) of
         % if table does not exists - create it and try to log message again
         {aborted,{no_exists, _Table}} ->
             case create_msg_table(VHost, Date) of
                  {aborted, CReason} ->
                     ?ERROR_MSG("Failed to log message: ~p", [CReason]),
                     error;
                  {atomic, ok} ->
                     ?MYDEBUG("Created msg table for ~s at ~s", [VHost, Date]),
                     log_message_int(VHost, MsgBin)
             end;
         {aborted, TReason} ->
             ?ERROR_MSG("Failed to log message: ~p", [TReason]),
             error;
         {atomic, _} ->
             ?MYDEBUG("Logged ok for ~s, peer: ~s", [ [Msg#msg.owner_name, <<"@">>, VHost],
                                                      [Msg#msg.peer_name, <<"@">>, Msg#msg.peer_server] ]),
             increment_user_stats(Msg#msg.owner_name, VHost, Date)
    end.

increment_user_stats(Owner, VHost, Date) ->
    Fun = fun() ->
            Pat = #stats{user=Owner, at=Date, count='$1'},
            mnesia:write_lock_table(stats_table(VHost)),
            case mnesia:select(stats_table(VHost), [{Pat, [], ['$_']}]) of
                 [] ->
                    mnesia:write(stats_table(VHost),
                                 #stats{user=Owner,
                                        at=Date,
                                        count=1},
                                 write);
                 [Stats] ->
                    mnesia:delete_object(stats_table(VHost),
                                         #stats{user=Owner,
                                                at=Date,
                                                count=Stats#stats.count},
                                         write),
                    New = Stats#stats{count = Stats#stats.count+1},
                    if
                      New#stats.count > 0 -> mnesia:write(stats_table(VHost),
                                                          New,
                                                          write);
                      true -> ok
                    end
            end
          end,
    case mnesia:transaction(Fun) of
         {aborted, Reason} ->
             ?ERROR_MSG("Failed to update stats for ~s@~s: ~p", [Owner, VHost, Reason]),
             error;
         {atomic, _} ->
             ?MYDEBUG("Updated stats for ~s@~s", [Owner, VHost]),
             ok
    end.

get_dates_int(VHost) ->
    Tables = mnesia:system_info(tables),
    lists:foldl(fun(ATable, Dates) ->
                    Table = term_to_binary(ATable),
                    case ejabberd_regexp:run( Table, << VHost/binary, <<"$">>/binary >> ) of
                         match ->
                            case re:run(Table, "[0-9]+-[0-9]+-[0-9]+") of
                                 {match, [{S, E}]} ->
                                     lists:append(Dates, [lists:sublist(binary_to_list(Table), S+1, E)]);
                                 nomatch ->
                                     Dates
                            end;
                         nomatch ->
                            Dates
                    end
                end, [], Tables).

rebuild_stats_at_int(VHost, Date) ->
    Table = table_name(VHost, Date),
    STable = stats_table(VHost),
    CFun = fun(Msg, Stats) ->
               Owner = Msg#msg.owner_name,
               case lists:keysearch(Owner, 1, Stats) of
                    {value, {_, Count}} ->
                       lists:keyreplace(Owner, 1, Stats, {Owner, Count + 1});
                    false ->
                       lists:append(Stats, [{Owner, 1}])
               end
           end,
    DFun = fun(#stats{at=SDate} = Stat, _Acc)
                when SDate == Date ->
                 mnesia:delete_object(stats_table(VHost), Stat, write);
              (_Stat, _Acc) -> ok
           end,
    % TODO: Maybe unregister hooks ?
    case mnesia:transaction(fun() ->
                               mnesia:write_lock_table(Table),
                               mnesia:write_lock_table(STable),
                               % Delete all stats for VHost at Date
                               mnesia:foldl(DFun, [], STable),
                               % Calc stats for VHost at Date
                               case mnesia:foldl(CFun, [], Table) of
                                    [] -> empty;
                                    AStats ->
                                      % Write new calc'ed stats
                                      lists:foreach(fun({Owner, Count}) ->
                                                        WStat = #stats{user=Owner, at=Date, count=Count},
                                                        mnesia:write(stats_table(VHost), WStat, write)
                                                    end, AStats),
                                      ok
                               end
                            end) of
         {aborted, Reason} ->
              ?ERROR_MSG("Failed to rebuild_stats_at for ~p at ~p: ~p", [VHost, Date, Reason]),
              error;
         {atomic, ok} ->
              ok;
         {atomic, empty} ->
              {atomic,ok} = mnesia:delete_table(Table),
              ?MYDEBUG("Dropped table at ~p", [Date]),
              ok
    end.

delete_nonexistent_stats(VHost) ->
    Dates = get_dates_int(VHost),
    mnesia:transaction(fun() ->
                          mnesia:foldl(fun(#stats{at=Date} = Stat, _Acc) ->
                                          case lists:member(Date, Dates) of
                                               false -> mnesia:delete_object(Stat);
                                               true -> ok
                                          end
                                       end, ok, stats_table(VHost))
                       end).

delete_stats_by_vhost_at_int(VHost, Date) ->
    StatsDelete = fun(#stats{at=SDate} = Stat, _Acc)
                      when SDate == Date ->
                        mnesia:delete_object(stats_table(VHost), Stat, write),
                        ok;
                     (_Msg, _Acc) -> ok
                  end,
    case mnesia:transaction(fun() ->
                             mnesia:write_lock_table(stats_table(VHost)),
                             mnesia:foldl(StatsDelete, ok, stats_table(VHost))
                       end) of
         {aborted, Reason} ->
            ?ERROR_MSG("Failed to update stats at ~p for ~p: ~p", [Date, VHost, Reason]),
            rebuild_stats_at_int(VHost, Date);
         _ ->
            ?INFO_MSG("Updated stats at ~p for ~p", [Date, VHost]),
            ok
    end.

get_user_stats_int(User, VHost) ->
    case mnesia:transaction(fun() ->
                               Pat = #stats{user=User, at='$1', count='$2'},
                               mnesia:select(stats_table(VHost), [{Pat, [], [['$1', '$2']]}])
                            end) of
         {atomic, Result} ->
                  {ok, mod_logdb:sort_stats([{Date, Count} || [Date, Count] <- Result])};
         {aborted, Reason} ->
                  {error, Reason}
    end.

delete_all_messages_by_user_at_int(User, VHost, Date) ->
    Table = table_name(VHost, Date),
    MsgDelete = fun(#msg{owner_name=Owner} = Msg, _Acc)
                     when Owner == User ->
                       mnesia:delete_object(Table, Msg, write),
                       ok;
                   (_Msg, _Acc) -> ok
                end,
    DRez = case mnesia:transaction(fun() ->
                                     mnesia:foldl(MsgDelete, ok, Table)
                                   end) of
                {aborted, Reason} ->
                   ?ERROR_MSG("Failed to delete_all_messages_by_user_at for ~p@~p at ~p: ~p", [User, VHost, Date, Reason]),
                   error;
                _ ->
                   ok
    end,
    case rebuild_stats_at_int(VHost, Date) of
         error ->
           error;
         ok ->
           DRez
    end.

delete_user_settings_int(User, VHost) ->
    STable = settings_table(VHost),
    case mnesia:dirty_match_object(STable, #user_settings{owner_name=User, _='_'}) of
         [] ->
            ok;
         [UserSettings] ->
            mnesia:dirty_delete_object(STable, UserSettings)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% tables internals
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
create_stats_table(VHost) ->
    SName = stats_table(VHost),
    case mnesia:create_table(SName,
                             [{disc_only_copies, [node()]},
                              {type, bag},
                              {attributes, record_info(fields, stats)},
                              {record_name, stats}
                             ]) of
         {atomic, ok} ->
             ?MYDEBUG("Created stats table for ~p", [VHost]),
             lists:foreach(fun(Date) ->
                    rebuild_stats_at_int(VHost, Date)
             end, get_dates_int(VHost)),
             ok;
         {aborted, {already_exists, _}} ->
             ?MYDEBUG("Stats table for ~p already exists", [VHost]),
             ok;
         {aborted, Reason} ->
             ?ERROR_MSG("Failed to create stats table: ~p", [Reason]),
             error
    end.

create_settings_table(VHost) ->
    SName = settings_table(VHost),
    case mnesia:create_table(SName,
                             [{disc_copies, [node()]},
                              {type, set},
                              {attributes, record_info(fields, user_settings)},
                              {record_name, user_settings}
                             ]) of
         {atomic, ok} ->
             ?MYDEBUG("Created settings table for ~p", [VHost]),
             ok;
         {aborted, {already_exists, _}} ->
             ?MYDEBUG("Settings table for ~p already exists", [VHost]),
             ok;
         {aborted, Reason} ->
             ?ERROR_MSG("Failed to create settings table: ~p", [Reason]),
             error
    end.

create_msg_table(VHost, Date) ->
    mnesia:create_table(
              table_name(VHost, Date),
              [{disc_only_copies, [node()]},
               {type, bag},
               {attributes, record_info(fields, msg)},
               {record_name, msg}]).
