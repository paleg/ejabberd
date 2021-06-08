%%%----------------------------------------------------------------------
%%% File    : mod_logdb.erl
%%% Author  : Oleg Palij (mailto:o.palij@gmail.com)
%%% Purpose : Frontend for log user messages to db
%%% Url     : https://paleg.github.io/mod_logdb/
%%%----------------------------------------------------------------------

-module(mod_logdb).
-author('o.palij@gmail.com').

-behaviour(gen_server).
-behaviour(gen_mod).

% supervisor
-export([start_link/2]).
% gen_mod
-export([start/2, stop/1,
         mod_opt_type/1,
         depends/2, reload/3,
         mod_doc/0, mod_options/1]).
% gen_server
-export([code_change/3,
         handle_call/3, handle_cast/2, handle_info/2,
         init/1, terminate/2]).
% hooks
-export([send_packet/1, receive_packet/1, offline_message/1, remove_user/2]).
-export([get_local_identity/5,
         get_local_features/5,
         get_local_items/5,
         adhoc_local_items/4,
         adhoc_local_commands/4
        ]).
% ejabberdctl
-export([rebuild_stats/1,
         copy_messages/1, copy_messages_ctl/3, copy_messages_int_tc/1]).
%
-export([get_vhost_stats/1, get_vhost_stats_at/2,
         get_user_stats/2, get_user_messages_at/3,
         get_dates/1,
         sort_stats/1,
         convert_timestamp/1, convert_timestamp_brief/1,
         get_user_settings/2, set_user_settings/3,
         user_messages_at_parse_query/4, user_messages_parse_query/3,
         vhost_messages_parse_query/2, vhost_messages_at_parse_query/4,
         list_to_bool/1, bool_to_list/1,
         list_to_string/1, string_to_list/1,
         get_module_settings/1, set_module_settings/2,
         purge_old_records/2]).
% webadmin hooks
-export([webadmin_menu/3,
         webadmin_user/4,
         webadmin_page/3,
         user_parse_query/5]).
% webadmin queries
-export([vhost_messages_stats/3,
         vhost_messages_stats_at/4,
         user_messages_stats/4,
         user_messages_stats_at/5]).

-include("mod_logdb.hrl").
-include_lib("xmpp/include/xmpp.hrl").
-include("mod_roster.hrl").
-include("ejabberd_commands.hrl").
-include("adhoc.hrl").
-include("ejabberd_web_admin.hrl").
-include("ejabberd_http.hrl").
-include("logger.hrl").

-define(PROCNAME, ejabberd_mod_logdb).
% gen_server call timeout
-define(CALL_TIMEOUT, 10000).

-record(state, {vhost, dbmod, backendPid, monref, purgeRef, pollRef, dbopts, dbs, dolog_default, ignore_jids, groupchat, purge_older_days, poll_users_settings, drop_messages_on_user_removal}).

ets_settings_table(VHost) -> list_to_atom("ets_logdb_settings_" ++ binary_to_list(VHost)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% gen_mod/gen_server callbacks
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% ejabberd starts module
start(VHost, Opts) ->
    ChildSpec =
        {gen_mod:get_module_proc(VHost, ?PROCNAME),
         {?MODULE, start_link, [VHost, Opts]},
         permanent,
         1000,
         worker,
         [?MODULE]},
    % add child to ejabberd_sup
    supervisor:start_child(ejabberd_gen_mod_sup, ChildSpec).

depends(_Host, _Opts) ->
    [].

reload(_Host, _NewOpts, _OldOpts) ->
    % TODO
    ok.

% supervisor starts gen_server
start_link(VHost, Opts) ->
    Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
    {ok, Pid} = gen_server:start_link({local, Proc}, ?MODULE, [VHost, Opts], []),
    Pid ! start,
    {ok, Pid}.

init([VHost, Opts]) ->
    process_flag(trap_exit, true),
    DBsRaw = gen_mod:get_opt(dbs, Opts),
    DBs = case lists:keysearch(mnesia, 1, DBsRaw) of
               false -> lists:append(DBsRaw, [{mnesia,[]}]);
               {value, _} -> DBsRaw
          end,
    VHostDB = gen_mod:get_opt(vhosts, Opts),
    % 10 is default because of using in clustered environment
    PollUsersSettings = gen_mod:get_opt(poll_users_settings, Opts),

    {DBName, DBOpts} =
         case lists:keysearch(VHost, 1, VHostDB) of
              false ->
                 ?WARNING_MSG("There is no logging backend defined for '~s', switching to mnesia", [VHost]),
                 {mnesia, []};
              {value,{_, DBNameResult}} ->
                 case lists:keysearch(DBNameResult, 1, DBs) of
                      false ->
                        ?WARNING_MSG("There is no such logging backend '~s' defined for '~s', switching to mnesia", [DBNameResult, VHost]),
                        {mnesia, []};
                      {value, {_, DBOptsResult}} ->
                        {DBNameResult, DBOptsResult}
                 end
         end,

    ?MYDEBUG("Starting mod_logdb for '~s' with '~s' backend", [VHost, DBName]),

    DBMod = list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(DBName)),

    {ok, #state{vhost=VHost,
                dbmod=DBMod,
                dbopts=DBOpts,
                % dbs used for convert messages from one backend to other
                dbs=DBs,
                dolog_default=gen_mod:get_opt(dolog_default, Opts),
                drop_messages_on_user_removal=gen_mod:get_opt(drop_messages_on_user_removal, Opts),
                ignore_jids=gen_mod:get_opt(ignore_jids, Opts),
                groupchat=gen_mod:get_opt(groupchat, Opts),
                purge_older_days=gen_mod:get_opt(purge_older_days, Opts),
                poll_users_settings=PollUsersSettings}}.

cleanup(#state{vhost=VHost} = _State) ->
    ?MYDEBUG("Stopping ~s for ~p", [?MODULE, VHost]),

    %ets:delete(ets_settings_table(VHost)),

    ejabberd_hooks:delete(remove_user, VHost, ?MODULE, remove_user, 90),
    ejabberd_hooks:delete(user_send_packet, VHost, ?MODULE, send_packet, 90),
    ejabberd_hooks:delete(user_receive_packet, VHost, ?MODULE, receive_packet, 90),
    ejabberd_hooks:delete(offline_message_hook, VHost, ?MODULE, offline_message, 40),

    ejabberd_hooks:delete(adhoc_local_commands, VHost, ?MODULE, adhoc_local_commands, 50),
    ejabberd_hooks:delete(adhoc_local_items, VHost, ?MODULE, adhoc_local_items, 50),
    ejabberd_hooks:delete(disco_local_identity, VHost, ?MODULE, get_local_identity, 50),
    ejabberd_hooks:delete(disco_local_features, VHost, ?MODULE, get_local_features, 50),
    ejabberd_hooks:delete(disco_local_items, VHost, ?MODULE, get_local_items, 50),

    ejabberd_hooks:delete(webadmin_menu_host, VHost, ?MODULE, webadmin_menu, 70),
    ejabberd_hooks:delete(webadmin_user, VHost, ?MODULE, webadmin_user, 50),
    ejabberd_hooks:delete(webadmin_page_host, VHost, ?MODULE, webadmin_page, 50),
    ejabberd_hooks:delete(webadmin_user_parse_query, VHost, ?MODULE, user_parse_query, 50),

    ?MYDEBUG("Removed hooks for ~p", [VHost]),

    ejabberd_commands:unregister_commands(get_commands_spec()),
    ?MYDEBUG("Unregistered commands for ~p", [VHost]).

stop(VHost) ->
    Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
    %gen_server:call(Proc, {cleanup}),
    %?MYDEBUG("Cleanup in stop finished!!!!", []),
    %timer:sleep(10000),
    ok = supervisor:terminate_child(ejabberd_gen_mod_sup, Proc),
    ok = supervisor:delete_child(ejabberd_gen_mod_sup, Proc).

get_commands_spec() ->
    [#ejabberd_commands{name = rebuild_stats, tags = [logdb],
            desc = "Rebuild mod_logdb stats for given host",
            module = ?MODULE, function = rebuild_stats,
            args = [{host, binary}],
            result = {res, rescode}},
     #ejabberd_commands{name = copy_messages, tags = [logdb],
            desc = "Copy logdb messages from given backend to current backend for given host",
            module = ?MODULE, function = copy_messages_ctl,
            args = [{host, binary}, {backend, binary}, {date, binary}],
            result = {res, rescode}}].

mod_options(Host) ->
    [{dbs, [{mnesia, []}]},
     {vhosts, [{Host, mnesia}]},
     {poll_users_settings, 10},
     {dolog_default, true},
     {drop_messages_on_user_removal, true},
     {ignore_jids, []},
     {groupchat, none},
     {purge_older_days, never}].

mod_doc() ->
    #{desc =>
           <<"This module adds support for "
             "logging user messages into database "
             "(mnesia, mysql, pgsql are now supported)">>}.

mod_opt_type(dbs) ->
    fun (A) when is_list(A) -> A end;
mod_opt_type(vhosts) ->
    fun (A) when is_list(A) -> A end;
mod_opt_type(poll_users_settings) ->
    fun (I) when is_integer(I) -> I end;
mod_opt_type(groupchat) ->
    fun (all) -> all;
        (send) -> send;
        (none) -> none
    end;
mod_opt_type(dolog_default) ->
    fun (B) when is_boolean(B) -> B end;
mod_opt_type(ignore_jids) ->
    fun (A) when is_list(A) -> A end;
mod_opt_type(purge_older_days) ->
    fun (I) when is_integer(I) -> I end;
mod_opt_type(_) ->
    [dbs, vhosts, poll_users_settings, groupchat, dolog_default, ignore_jids, purge_older_days].

handle_call({cleanup}, _From, State) ->
    cleanup(State),
    ?MYDEBUG("Cleanup finished!!!!!", []),
    {reply, ok, State};
handle_call({get_dates}, _From, #state{dbmod=DBMod, vhost=VHost}=State) ->
    Reply = DBMod:get_dates(VHost),
    {reply, Reply, State};
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% ejabberd_web_admin callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_call({delete_messages_by_user_at, PMsgs, Date}, _From, #state{dbmod=DBMod, vhost=VHost}=State) ->
    Reply = DBMod:delete_messages_by_user_at(VHost, PMsgs, binary_to_list(Date)),
    {reply, Reply, State};
handle_call({delete_all_messages_by_user_at, User, Date}, _From, #state{dbmod=DBMod, vhost=VHost}=State) ->
    Reply = DBMod:delete_all_messages_by_user_at(binary_to_list(User), VHost, binary_to_list(Date)),
    {reply, Reply, State};
handle_call({delete_messages_at, Date}, _From, #state{dbmod=DBMod, vhost=VHost}=State) ->
    Reply = DBMod:delete_messages_at(VHost, Date),
    {reply, Reply, State};
handle_call({get_vhost_stats}, _From, #state{dbmod=DBMod, vhost=VHost}=State) ->
    Reply = DBMod:get_vhost_stats(VHost),
    {reply, Reply, State};
handle_call({get_vhost_stats_at, Date}, _From, #state{dbmod=DBMod, vhost=VHost}=State) ->
    Reply = DBMod:get_vhost_stats_at(VHost, binary_to_list(Date)),
    {reply, Reply, State};
handle_call({get_user_stats, User}, _From, #state{dbmod=DBMod, vhost=VHost}=State) ->
    Reply = DBMod:get_user_stats(binary_to_list(User), VHost),
    {reply, Reply, State};
handle_call({get_user_messages_at, User, Date}, _From, #state{dbmod=DBMod, vhost=VHost}=State) ->
    Reply = DBMod:get_user_messages_at(binary_to_list(User), VHost, binary_to_list(Date)),
    {reply, Reply, State};
handle_call({get_user_settings, User}, _From, #state{dbmod=_DBMod, vhost=VHost}=State) ->
    Reply = case ets:match_object(ets_settings_table(VHost),
                                  #user_settings{owner_name=User, _='_'}) of
                 [Set] -> Set;
                 _ -> #user_settings{owner_name=User,
                                     dolog_default=State#state.dolog_default,
                                     dolog_list=[],
                                     donotlog_list=[]}
            end,
    {reply, Reply, State};
% TODO: remove User ??
handle_call({set_user_settings, User, GSet}, _From, #state{dbmod=DBMod, vhost=VHost}=State) ->
    Set = GSet#user_settings{owner_name=User},
    Reply =
       case ets:match_object(ets_settings_table(VHost),
                             #user_settings{owner_name=User, _='_'}) of
            [Set] ->
                ok;
            _ ->
                case DBMod:set_user_settings(binary_to_list(User), VHost, Set) of
                     error ->
                       error;
                     ok ->
                       true = ets:insert(ets_settings_table(VHost), Set),
                       ok
                end
       end,
    {reply, Reply, State};
handle_call({get_module_settings}, _From, State) ->
    {reply, State, State};
handle_call({set_module_settings, #state{purge_older_days=PurgeDays,
                                         poll_users_settings=PollSec} = Settings},
            _From,
            #state{purgeRef=PurgeRefOld,
                   pollRef=PollRefOld,
                   purge_older_days=PurgeDaysOld,
                   poll_users_settings=PollSecOld} = State) ->
    PurgeRef = if
                 PurgeDays == never, PurgeDaysOld /= never  ->
                    {ok, cancel} = timer:cancel(PurgeRefOld),
                    disabled;
                 is_integer(PurgeDays), PurgeDaysOld == never ->
                    set_purge_timer(PurgeDays);
                 true ->
                    PurgeRefOld
               end,

    PollRef = if
                PollSec == PollSecOld ->
                   PollRefOld;
                PollSec == 0, PollSecOld /= 0 ->
                   {ok, cancel} = timer:cancel(PollRefOld),
                   disabled;
                is_integer(PollSec), PollSecOld == 0 ->
                   set_poll_timer(PollSec);
                is_integer(PollSec), PollSecOld /= 0 ->
                   {ok, cancel} = timer:cancel(PollRefOld),
                   set_poll_timer(PollSec)
              end,

    NewState = State#state{dolog_default=Settings#state.dolog_default,
                           ignore_jids=Settings#state.ignore_jids,
                           groupchat=Settings#state.groupchat,
                           drop_messages_on_user_removal=Settings#state.drop_messages_on_user_removal,
                           purge_older_days=PurgeDays,
                           poll_users_settings=PollSec,
                           purgeRef=PurgeRef,
                           pollRef=PollRef},
    {reply, ok, NewState};
handle_call(Msg, _From, State) ->
    ?INFO_MSG("Got call Msg: ~p, State: ~p", [Msg, State]),
    {noreply, State}.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% end ejabberd_web_admin callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% ejabberd_hooks call
handle_cast({addlog, Direction, Owner, Peer, Packet}, #state{dbmod=DBMod, vhost=VHost}=State) ->
    case filter(Owner, Peer, State) of
         true ->
              case catch packet_parse(Owner, Peer, Packet, Direction, State) of
                   ignore ->
                     ok;
                   {'EXIT', Reason} ->
                     ?ERROR_MSG("Failed to parse: ~p", [Reason]);
                   Msg ->
                     DBMod:log_message(VHost, Msg)
              end;
         false ->
              ok
    end,
    {noreply, State};
handle_cast({remove_user, User}, #state{dbmod=DBMod, vhost=VHost}=State) ->
    case State#state.drop_messages_on_user_removal of
         true ->
           DBMod:drop_user(binary_to_list(User), VHost),
           ?INFO_MSG("Launched ~s@~s removal", [User, VHost]);
         false ->
           ?INFO_MSG("Message removing is disabled. Keeping messages for ~s@~s", [User, VHost])
    end,
    {noreply, State};
% ejabberdctl rebuild_stats/3
handle_cast({rebuild_stats}, #state{dbmod=DBMod, vhost=VHost}=State) ->
    DBMod:rebuild_stats(VHost),
    {noreply, State};
handle_cast({copy_messages, Backend}, State) ->
    spawn(?MODULE, copy_messages, [[State, Backend, []]]),
    {noreply, State};
handle_cast({copy_messages, Backend, Date}, State) ->
    spawn(?MODULE, copy_messages, [[State, Backend, [binary_to_list(Date)]]]),
    {noreply, State};
handle_cast(Msg, State) ->
    ?INFO_MSG("Got cast Msg:~p, State:~p", [Msg, State]),
    {noreply, State}.

% return: disabled | timer reference
set_purge_timer(PurgeDays) ->
    case PurgeDays of
         never -> disabled;
         Days when is_integer(Days) ->
              {ok, Ref1} = timer:send_interval(timer:hours(24), scheduled_purging),
              Ref1
    end.

% return: disabled | timer reference
set_poll_timer(PollSec) ->
    if
      PollSec > 0 ->
        {ok, Ref2} = timer:send_interval(timer:seconds(PollSec), poll_users_settings),
        Ref2;
      % db polling disabled
      PollSec == 0 ->
        disabled;
      true ->
        {ok, Ref3} = timer:send_interval(timer:seconds(10), poll_users_settings),
        Ref3
    end.

% actual starting of logging
% from timer:send_after (in init)
handle_info(start, #state{dbmod=DBMod, vhost=VHost}=State) ->
    case DBMod:start(VHost, State#state.dbopts) of
         {error,{already_started,_}} ->
           ?MYDEBUG("backend module already started - trying to stop it", []),
           DBMod:stop(VHost),
           {stop, already_started, State};
         {error, Reason} ->
           timer:sleep(30000),
           ?ERROR_MSG("Failed to start: ~p", [Reason]),
           {stop, db_connection_failed, State};
         {ok, SPid} ->
           ?INFO_MSG("~p connection established", [DBMod]),

           MonRef = erlang:monitor(process, SPid),

           ets:new(ets_settings_table(VHost), [named_table,public,set,{keypos, #user_settings.owner_name}]),
           DoLog = case DBMod:get_users_settings(VHost) of
                        {ok, Settings} -> [Sett#user_settings{owner_name = iolist_to_binary(Sett#user_settings.owner_name)} || Sett <- Settings];
                        {error, _Reason} -> []
                   end,
           ets:insert(ets_settings_table(VHost), DoLog),

           TrefPurge = set_purge_timer(State#state.purge_older_days),
           TrefPoll = set_poll_timer(State#state.poll_users_settings),

           ejabberd_hooks:add(remove_user, VHost, ?MODULE, remove_user, 90),
           ejabberd_hooks:add(user_send_packet, VHost, ?MODULE, send_packet, 90),
           ejabberd_hooks:add(user_receive_packet, VHost, ?MODULE, receive_packet, 90),
           ejabberd_hooks:add(offline_message_hook, VHost, ?MODULE, offline_message, 40),

           ejabberd_hooks:add(adhoc_local_commands, VHost, ?MODULE, adhoc_local_commands, 50),
           ejabberd_hooks:add(disco_local_items, VHost, ?MODULE, get_local_items, 50),
           ejabberd_hooks:add(disco_local_identity, VHost, ?MODULE, get_local_identity, 50),
           ejabberd_hooks:add(disco_local_features, VHost, ?MODULE, get_local_features, 50),
           ejabberd_hooks:add(adhoc_local_items, VHost, ?MODULE, adhoc_local_items, 50),

           ejabberd_hooks:add(webadmin_menu_host, VHost, ?MODULE, webadmin_menu, 70),
           ejabberd_hooks:add(webadmin_user, VHost, ?MODULE, webadmin_user, 50),
           ejabberd_hooks:add(webadmin_page_host, VHost, ?MODULE, webadmin_page, 50),
           ejabberd_hooks:add(webadmin_user_parse_query, VHost, ?MODULE, user_parse_query, 50),

           ?MYDEBUG("Added hooks for ~p", [VHost]),

           ejabberd_commands:register_commands(get_commands_spec()),
           ?MYDEBUG("Registered commands for ~p", [VHost]),

           NewState=State#state{monref = MonRef, backendPid=SPid, purgeRef=TrefPurge, pollRef=TrefPoll},
           {noreply, NewState};
        Rez ->
           ?ERROR_MSG("Rez=~p", [Rez]),
           timer:sleep(30000),
           {stop, db_connection_failed, State}
    end;
% from timer:send_interval/2 (in start handle_info)
handle_info(scheduled_purging, #state{vhost=VHost, purge_older_days=Days} = State) ->
    ?MYDEBUG("Starting scheduled purging of old records for ~p", [VHost]),
    spawn(?MODULE, purge_old_records, [VHost, integer_to_list(Days)]),
    {noreply, State};
% from timer:send_interval/2 (in start handle_info)
handle_info(poll_users_settings, #state{dbmod=DBMod, vhost=VHost}=State) ->
    {ok, DoLog} = DBMod:get_users_settings(VHost),
    ?MYDEBUG("DoLog=~p", [DoLog]),
    true = ets:delete_all_objects(ets_settings_table(VHost)),
    ets:insert(ets_settings_table(VHost), DoLog),
    {noreply, State};
handle_info({'DOWN', _MonitorRef, process, _Pid, _Info}, State) ->
    {stop, db_connection_dropped, State};
handle_info({fetch_result, _, _}, State) ->
    ?MYDEBUG("Got timed out mysql fetch result", []),
    {noreply, State};
handle_info(Info, State) ->
    ?INFO_MSG("Got Info:~p, State:~p", [Info, State]),
    {noreply, State}.

terminate(db_connection_failed, _State) ->
    ok;
terminate(db_connection_dropped, State) ->
    ?MYDEBUG("Got terminate with db_connection_dropped", []),
    cleanup(State),
    ok;
terminate(Reason, #state{monref=undefined} = State) ->
    ?MYDEBUG("Got terminate with undefined monref.~nReason: ~p", [Reason]),
    cleanup(State),
    ok;
terminate(Reason, #state{dbmod=DBMod, vhost=VHost, monref=MonRef, backendPid=Pid} = State) ->
    ?INFO_MSG("Reason: ~p", [Reason]),
    case erlang:is_process_alive(Pid) of
         true ->
           erlang:demonitor(MonRef, [flush]),
           DBMod:stop(VHost);
         false ->
           ok
    end,
    cleanup(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ejabberd_hooks callbacks
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% TODO: change to/from to list as sql stores it as list
send_packet({Pkt, #{jid := Owner} = C2SState}) ->
    VHost = Owner#jid.lserver,
    Peer = xmpp:get_to(Pkt),
    %?MYDEBUG("send_packet. Peer=~p, Owner=~p", [Peer, Owner]),
    Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
    gen_server:cast(Proc, {addlog, to, Owner, Peer, Pkt}),
    {Pkt, C2SState}.

receive_packet({Pkt, #{jid := Owner} = C2SState}) ->
    VHost = Owner#jid.lserver,
    Peer = xmpp:get_from(Pkt),
    %?MYDEBUG("receive_packet. Pkt=~p", [Pkt]),
    Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
    gen_server:cast(Proc, {addlog, from, Owner, Peer, Pkt}),
    {Pkt, C2SState}.

offline_message({_Action, #message{from = Peer, to = Owner} = Pkt} = Acc) ->
    VHost = Owner#jid.lserver,
    %?MYDEBUG("offline_message. Pkt=~p", [Pkt]),
    Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
    gen_server:cast(Proc, {addlog, from, Owner, Peer, Pkt}),
    Acc.

remove_user(User, Server) ->
    LUser = jid:nodeprep(User),
    LServer = jid:nameprep(Server),
    Proc = gen_mod:get_module_proc(LServer, ?PROCNAME),
    gen_server:cast(Proc, {remove_user, LUser}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ejabberdctl
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
rebuild_stats(VHost) ->
    Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
    gen_server:cast(Proc, {rebuild_stats}),
    ok.

copy_messages_ctl(VHost, Backend, <<"all">>) ->
    Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
    gen_server:cast(Proc, {copy_messages, Backend}),
    ok;
copy_messages_ctl(VHost, Backend, Date) ->
    Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
    gen_server:cast(Proc, {copy_messages, Backend, Date}),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% misc operations
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% handle_cast({addlog, E}, _)
% raw packet -> #msg
packet_parse(_Owner, _Peer, #message{type = error}, _Direction, _State) ->
    ignore;
packet_parse(_Owner, _Peer, #message{meta = #{sm_copy := true}}, _Direction, _State) ->
    ignore;
packet_parse(_Owner, _Peer, #message{meta = #{from_offline := true}}, _Direction, _State) ->
    ignore;
packet_parse(Owner, Peer, #message{body = Body, subject = Subject, type = Type}, Direction, State) ->
    %?MYDEBUG("Owner=~p, Peer=~p, Direction=~p", [Owner, Peer, Direction]),
    %?MYDEBUG("Body=~p, Subject=~p, Type=~p", [Body, Subject, Type]),
    SubjectText = xmpp:get_text(Subject),
    BodyText = xmpp:get_text(Body),
    if (SubjectText == <<"">>) and (BodyText == <<"">>) ->
        throw(ignore);
       true -> ok
    end,

    case Type of
         groupchat when State#state.groupchat == send, Direction == to ->
            ok;
         groupchat when State#state.groupchat == send, Direction == from ->
            throw(ignore);
         groupchat when State#state.groupchat == none ->
            throw(ignore);
         _ ->
            ok
    end,

    #msg{timestamp     = get_timestamp(),
         owner_name    = stringprep:tolower(Owner#jid.user),
         peer_name     = stringprep:tolower(Peer#jid.user),
         peer_server   = stringprep:tolower(Peer#jid.server),
         peer_resource = Peer#jid.resource,
         direction     = Direction,
         type          = misc:atom_to_binary(Type),
         subject       = SubjectText,
         body          = BodyText};
packet_parse(_, _, _, _, _) ->
    ignore.

% called from handle_cast({addlog, _}, _) -> true (log messages) | false (do not log messages)
filter(Owner, Peer, State) ->
    OwnerBin = << (Owner#jid.luser)/binary, "@", (Owner#jid.lserver)/binary >>,
    OwnerServ = << "@", (Owner#jid.lserver)/binary >>,
    PeerBin = << (Peer#jid.luser)/binary, "@", (Peer#jid.lserver)/binary >>,
    PeerServ = << "@", (Peer#jid.lserver)/binary >>,

    LogTo = case ets:match_object(ets_settings_table(State#state.vhost),
                                  #user_settings{owner_name=Owner#jid.luser, _='_'}) of
                 [#user_settings{dolog_default=Default,
                                 dolog_list=DLL,
                                 donotlog_list=DNLL}] ->

                      A = lists:member(PeerBin, DLL),
                      B = lists:member(PeerBin, DNLL),
                      if
                        A -> true;
                        B -> false;
                        Default == true -> true;
                        Default == false -> false;
                        true -> State#state.dolog_default
                      end;
                 _ -> State#state.dolog_default
            end,
    lists:all(fun(O) -> O end,
              [not lists:member(OwnerBin, State#state.ignore_jids),
               not lists:member(PeerBin, State#state.ignore_jids),
               not lists:member(OwnerServ, State#state.ignore_jids),
               not lists:member(PeerServ, State#state.ignore_jids),
               LogTo]).

purge_old_records(VHost, Days) ->
    Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),

    Dates = ?MODULE:get_dates(VHost),
    DateNow = calendar:datetime_to_gregorian_seconds({date(), {0,0,1}}),
    DateDiff = list_to_integer(Days)*24*60*60,
    ?MYDEBUG("Purging tables older than ~s days", [Days]),
    lists:foreach(fun(Date) ->
                    [Year, Month, Day] = ejabberd_regexp:split(iolist_to_binary(Date), <<"[^0-9]+">>),
                    DateInSec = calendar:datetime_to_gregorian_seconds({{binary_to_integer(Year), binary_to_integer(Month), binary_to_integer(Day)}, {0,0,1}}),
                    if
                     (DateNow - DateInSec) > DateDiff ->
                        gen_server:call(Proc, {delete_messages_at, Date});
                     true ->
                        ?MYDEBUG("Skipping messages at ~p", [Date])
                    end
              end, Dates).

% called from get_vhost_stats/2, get_user_stats/3
sort_stats(Stats) ->
    % Stats = [{"2003-4-15",1}, {"2006-8-18",1}, ... ]
    CFun = fun({TableName, Count}) ->
                 [Year, Month, Day] = ejabberd_regexp:split(iolist_to_binary(TableName), <<"[^0-9]+">>),
                 { calendar:datetime_to_gregorian_seconds({{binary_to_integer(Year), binary_to_integer(Month), binary_to_integer(Day)}, {0,0,1}}), Count }
           end,
    % convert to [{63364377601,1}, {63360662401,1}, ... ]
    CStats = lists:map(CFun, Stats),
    % sort by date
    SortedStats = lists:reverse(lists:keysort(1, CStats)),
    % convert to [{"2007-12-9",1}, {"2007-10-27",1}, ... ] sorted list
    [{mod_logdb:convert_timestamp_brief(TableSec), Count} || {TableSec, Count} <- SortedStats].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% Date/Time operations
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% return float seconds elapsed from "zero hour" as list
get_timestamp() ->
    {MegaSec, Sec, MicroSec} = now(),
    [List] = io_lib:format("~.5f", [MegaSec*1000000 + Sec + MicroSec/1000000]),
    List.

% convert float seconds elapsed from "zero hour" to local time "%Y-%m-%d %H:%M:%S" string
convert_timestamp(Seconds) when is_list(Seconds) ->
    case string:to_float(Seconds++".0") of
         {F,_} when is_float(F) -> convert_timestamp(F);
         _ -> erlang:error(badarg, [Seconds])
    end;
convert_timestamp(Seconds) when is_float(Seconds) ->
    GregSec = trunc(Seconds + 719528*86400),
    UnivDT = calendar:gregorian_seconds_to_datetime(GregSec),
    {{Year, Month, Day},{Hour, Minute, Sec}} = calendar:universal_time_to_local_time(UnivDT),
    integer_to_list(Year) ++ "-" ++ integer_to_list(Month) ++ "-" ++ integer_to_list(Day) ++ " " ++ integer_to_list(Hour) ++ ":" ++ integer_to_list(Minute) ++ ":" ++ integer_to_list(Sec).

% convert float seconds elapsed from "zero hour" to local time "%Y-%m-%d" string
convert_timestamp_brief(Seconds) when is_list(Seconds) ->
    convert_timestamp_brief(list_to_float(Seconds));
convert_timestamp_brief(Seconds) when is_float(Seconds) ->
    GregSec = trunc(Seconds + 719528*86400),
    UnivDT = calendar:gregorian_seconds_to_datetime(GregSec),
    {{Year, Month, Day},{_Hour, _Minute, _Sec}} = calendar:universal_time_to_local_time(UnivDT),
    integer_to_list(Year) ++ "-" ++ integer_to_list(Month) ++ "-" ++ integer_to_list(Day);
convert_timestamp_brief(Seconds) when is_integer(Seconds) ->
    {{Year, Month, Day},{_Hour, _Minute, _Sec}} = calendar:gregorian_seconds_to_datetime(Seconds),
    integer_to_list(Year) ++ "-" ++ integer_to_list(Month) ++ "-" ++ integer_to_list(Day).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% DB operations (get)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
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

set_user_settings(User, VHost, Set) ->
    Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
    gen_server:call(Proc, {set_user_settings, User, Set}).

get_module_settings(VHost) ->
    Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
    gen_server:call(Proc, {get_module_settings}).

set_module_settings(VHost, Settings) ->
    Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
    gen_server:call(Proc, {set_module_settings, Settings}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% Web admin callbacks (delete)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
user_messages_at_parse_query(VHost, Date, Msgs, Query) ->
    case lists:keysearch(<<"delete">>, 1, Query) of
         {value, _} ->
             PMsgs = lists:filter(
                              fun(Msg) ->
                                   ID = misc:encode_base64(term_to_binary(Msg#msg.timestamp)),
                                   lists:member({<<"selected">>, ID}, Query)
                              end, Msgs),
             Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
             gen_server:call(Proc, {delete_messages_by_user_at, PMsgs, Date}, ?CALL_TIMEOUT);
         false ->
             nothing
    end.

user_messages_parse_query(User, VHost, Query) ->
    case lists:keysearch(<<"delete">>, 1, Query) of
         {value, _} ->
             Dates = get_dates(VHost),
             PDates = lists:filter(
                              fun(Date) ->
                                   ID = misc:encode_base64( << User/binary, (iolist_to_binary(Date))/binary >> ),
                                   lists:member({<<"selected">>, ID}, Query)
                              end, Dates),
             Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
             Rez = lists:foldl(
                          fun(Date, Acc) ->
                              lists:append(Acc,
                                           [gen_server:call(Proc,
                                                            {delete_all_messages_by_user_at, User, iolist_to_binary(Date)},
                                                            ?CALL_TIMEOUT)])
                          end, [], PDates),
             case lists:member(error, Rez) of
                  true ->
                    error;
                  false ->
                    nothing
             end;
         false ->
             nothing
    end.

vhost_messages_parse_query(VHost, Query) ->
    case lists:keysearch(<<"delete">>, 1, Query) of
         {value, _} ->
             Dates = get_dates(VHost),
             PDates = lists:filter(
                              fun(Date) ->
                                   ID = misc:encode_base64( << VHost/binary, (iolist_to_binary(Date))/binary >> ),
                                   lists:member({<<"selected">>, ID}, Query)
                              end, Dates),
             Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
             Rez = lists:foldl(fun(Date, Acc) ->
                                   lists:append(Acc, [gen_server:call(Proc,
                                                                      {delete_messages_at, Date},
                                                                      ?CALL_TIMEOUT)])
                               end, [], PDates),
             case lists:member(error, Rez) of
                  true ->
                    error;
                  false ->
                    nothing
             end;
         false ->
             nothing
    end.

vhost_messages_at_parse_query(VHost, Date, Stats, Query) ->
    case lists:keysearch(<<"delete">>, 1, Query) of
         {value, _} ->
             PStats = lists:filter(
                              fun({User, _Count}) ->
                                   ID = misc:encode_base64( << (iolist_to_binary(User))/binary, VHost/binary >> ),
                                   lists:member({<<"selected">>, ID}, Query)
                              end, Stats),
             Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
             Rez = lists:foldl(fun({User, _Count}, Acc) ->
                                   lists:append(Acc, [gen_server:call(Proc,
                                                                      {delete_all_messages_by_user_at,
                                                                       iolist_to_binary(User), iolist_to_binary(Date)},
                                                                      ?CALL_TIMEOUT)])
                               end, [], PStats),
             case lists:member(error, Rez) of
                  true ->
                    error;
                  false ->
                    ok
             end;
         false ->
             nothing
    end.

copy_messages([#state{vhost=VHost}=State, From, DatesIn]) ->
    {FromDBName, FromDBOpts} =
         case lists:keysearch(misc:binary_to_atom(From), 1, State#state.dbs) of
              {value, {FN, FO}} ->
                 {FN, FO};
              false ->
                 ?ERROR_MSG("Failed to find record for ~p in dbs", [From]),
                 throw(error)
         end,

    FromDBMod = list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(FromDBName)),

    {ok, _FromPid} = FromDBMod:start(VHost, FromDBOpts),

    Dates = case DatesIn of
                 [] -> FromDBMod:get_dates(VHost);
                 _ -> DatesIn
            end,

    DatesLength = length(Dates),

    catch lists:foldl(fun(Date, Acc) ->
                        case catch copy_messages_int([FromDBMod, State#state.dbmod, VHost, Date]) of
                            ok ->
                                ?INFO_MSG("Copied messages at ~p (~p/~p)", [Date, Acc, DatesLength]);
                            Value ->
                                ?ERROR_MSG("Failed to copy messages at ~p (~p/~p): ~p", [Date, Acc, DatesLength, Value]),
                                throw(error)
                        end,
                        Acc + 1
                      end, 1, Dates),
    ?INFO_MSG("copy_messages from ~p finished",  [From]),
    FromDBMod:stop(VHost).

copy_messages_int([FromDBMod, ToDBMod, VHost, Date]) ->
    ets:new(mod_logdb_temp, [named_table, set, public]),
    {Time, Value} = timer:tc(?MODULE, copy_messages_int_tc, [[FromDBMod, ToDBMod, VHost, Date]]),
    ets:delete_all_objects(mod_logdb_temp),
    ets:delete(mod_logdb_temp),
    ?INFO_MSG("copy_messages at ~p elapsed ~p sec", [Date, Time/1000000]),
    Value.

copy_messages_int_tc([FromDBMod, ToDBMod, VHost, Date]) ->
    ?INFO_MSG("Going to copy messages from ~p for ~p at ~p", [FromDBMod, VHost, Date]),

    ok = FromDBMod:rebuild_stats_at(VHost, Date),
    catch mod_logdb:rebuild_stats_at(VHost, Date),
    {ok, FromStats} = FromDBMod:get_vhost_stats_at(VHost, Date),
    ToStats = case mod_logdb:get_vhost_stats_at(VHost, iolist_to_binary(Date)) of
                   {ok, Stats} -> Stats;
                   {error, _} -> []
              end,

    FromStatsS = lists:keysort(1, FromStats),
    ToStatsS = lists:keysort(1, ToStats),

    StatsLength = length(FromStats),

    CopyFun = if
                % destination table is empty
                ToStats == [] ->
                    fun({User, _Count}, Acc) ->
                        {ok, Msgs} = FromDBMod:get_user_messages_at(User, VHost, Date),
                        MAcc =
                          lists:foldl(fun(Msg, MFAcc) ->
                                          MsgBinary = Msg#msg{owner_name=iolist_to_binary(User),
                                                              peer_name=iolist_to_binary(Msg#msg.peer_name),
                                                              peer_server=iolist_to_binary(Msg#msg.peer_server),
                                                              peer_resource=iolist_to_binary(Msg#msg.peer_resource),
                                                              type=iolist_to_binary(Msg#msg.type),
                                                              subject=iolist_to_binary(Msg#msg.subject),
                                                              body=iolist_to_binary(Msg#msg.body)},
                                          ok = ToDBMod:log_message(VHost, MsgBinary),
                                          MFAcc + 1
                                      end, 0, Msgs),
                        NewAcc = Acc + 1,
                        ?INFO_MSG("Copied ~p messages for ~p (~p/~p) at ~p", [MAcc, User, NewAcc, StatsLength, Date]),
                        %timer:sleep(100),
                        NewAcc
                    end;
                % destination table is not empty
                true ->
                    fun({User, _Count}, Acc) ->
                        {ok, ToMsgs} = ToDBMod:get_user_messages_at(User, VHost, Date),
                        lists:foreach(fun(#msg{timestamp=Tst}) when length(Tst) == 16 ->
                                            ets:insert(mod_logdb_temp, {Tst});
                                         % mysql, pgsql removes final zeros after decimal point
                                         (#msg{timestamp=Tst}) when length(Tst) < 16 ->
                                            {F, _} = string:to_float(Tst++".0"),
                                            [T] = io_lib:format("~.5f", [F]),
                                            ets:insert(mod_logdb_temp, {T})
                                      end, ToMsgs),
                        {ok, Msgs} = FromDBMod:get_user_messages_at(User, VHost, Date),
                        MAcc =
                          lists:foldl(fun(#msg{timestamp=ToTimestamp} = Msg, MFAcc) ->
                                          case ets:member(mod_logdb_temp, ToTimestamp) of
                                               false ->
                                                  MsgBinary = Msg#msg{owner_name=iolist_to_binary(User),
                                                                      peer_name=iolist_to_binary(Msg#msg.peer_name),
                                                                      peer_server=iolist_to_binary(Msg#msg.peer_server),
                                                                      peer_resource=iolist_to_binary(Msg#msg.peer_resource),
                                                                      type=iolist_to_binary(Msg#msg.type),
                                                                      subject=iolist_to_binary(Msg#msg.subject),
                                                                      body=iolist_to_binary(Msg#msg.body)},
                                                  ok = ToDBMod:log_message(VHost, MsgBinary),
                                                  ets:insert(mod_logdb_temp, {ToTimestamp}),
                                                  MFAcc + 1;
                                               true ->
                                                  MFAcc
                                          end
                                      end, 0, Msgs),
                        NewAcc = Acc + 1,
                        ets:delete_all_objects(mod_logdb_temp),
                        ?INFO_MSG("Copied ~p messages for ~p (~p/~p) at ~p", [MAcc, User, NewAcc, StatsLength, Date]),
                        %timer:sleep(100),
                        NewAcc
                    end
              end,

    if
      FromStats == [] ->
        ?INFO_MSG("No messages were found at ~p", [Date]);
      FromStatsS == ToStatsS ->
        ?INFO_MSG("Stats are equal at ~p", [Date]);
      FromStatsS /= ToStatsS ->
        lists:foldl(CopyFun, 0, FromStats),
        ok = ToDBMod:rebuild_stats_at(VHost, Date)
        %timer:sleep(1000)
    end,

    ok.

list_to_bool(Num) when is_binary(Num) ->
    list_to_bool(binary_to_list(Num));
list_to_bool(Num) when is_list(Num) ->
    case lists:member(Num, ["t", "true", "y", "yes", "1"]) of
         true ->
           true;
         false ->
           case lists:member(Num, ["f", "false", "n", "no", "0"]) of
                true ->
                  false;
                false ->
                  error
           end
    end.

bool_to_list(true) ->
    "TRUE";
bool_to_list(false) ->
    "FALSE".

list_to_string([]) ->
    "";
list_to_string(List) when is_list(List) ->
    Str = lists:flatmap(fun(Elm) when is_binary(Elm) ->
                              binary_to_list(Elm) ++ "\n";
                           (Elm) when is_list(Elm) ->
                              Elm ++ "\n"
                        end, List),
    lists:sublist(Str, length(Str)-1).

string_to_list(null) ->
    [];
string_to_list([]) ->
    [];
string_to_list(String) ->
    ejabberd_regexp:split(iolist_to_binary(String), <<"\n">>).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ad-hoc (copy/pasted from mod_configure.erl)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-define(ITEMS_RESULT(Allow, LNode, Fallback),
    case Allow of
        deny -> Fallback;
        allow ->
            case get_local_items(LServer, LNode,
                                 jid:encode(To), Lang) of
                {result, Res} -> {result, Res};
                {error, Error} -> {error, Error}
            end
    end).

get_local_items(Acc, From, #jid{lserver = LServer} = To,
                <<"">>, Lang) ->
    case gen_mod:is_loaded(LServer, mod_adhoc) of
        false -> Acc;
        _ ->
            Items = case Acc of
                         {result, Its} -> Its;
                         empty -> []
                    end,
            AllowUser = acl:match_rule(LServer, mod_logdb, From),
            AllowAdmin = acl:match_rule(LServer, mod_logdb_admin, From),
            if
              AllowUser == allow; AllowAdmin == allow ->
                case get_local_items(LServer, [],
                                     jid:encode(To), Lang) of
                     {result, Res} ->
                        {result, Items ++ Res};
                     {error, _Error} ->
                        {result, Items}
                end;
              true ->
                {result, Items}
            end
    end;
get_local_items(Acc, From, #jid{lserver = LServer} = To,
                Node, Lang) ->
    case gen_mod:is_loaded(LServer, mod_adhoc) of
        false -> Acc;
        _ ->
            LNode = tokenize(Node),
            AllowAdmin = acl:match_rule(LServer, mod_logdb_admin, From),
            Err = xmpp:err_forbidden(<<"Denied by ACL">>, Lang),
            case LNode of
                 [<<"mod_logdb">>] ->
                      ?ITEMS_RESULT(AllowAdmin, LNode, {error, Err});
                 [<<"mod_logdb_users">>] ->
                      ?ITEMS_RESULT(AllowAdmin, LNode, {error, Err});
                 [<<"mod_logdb_users">>, <<$@, _/binary>>] ->
                      ?ITEMS_RESULT(AllowAdmin, LNode, {error, Err});
                 [<<"mod_logdb_users">>, _User] ->
                      ?ITEMS_RESULT(AllowAdmin, LNode, {error, Err});
                 [<<"mod_logdb_settings">>] ->
                      ?ITEMS_RESULT(AllowAdmin, LNode, {error, Err});
                 _ ->
                      Acc
            end
    end.

-define(T(Lang, Text), translate:translate(Lang, Text)).

-define(NODE(Name, Node),
    #disco_item{jid = jid:make(Server),
            node = Node,
            name = ?T(Lang, Name)}).

-define(NS_ADMINX(Sub),
    <<(?NS_ADMIN)/binary, "#", Sub/binary>>).

tokenize(Node) -> str:tokens(Node, <<"/#">>).

get_local_items(_Host, [], Server, Lang) ->
    {result,
     [?NODE(<<"Messages logging engine">>, <<"mod_logdb">>)]
    };
get_local_items(_Host, [<<"mod_logdb">>], Server, Lang) ->
    {result,
     [?NODE(<<"Messages logging engine users">>, <<"mod_logdb_users">>),
      ?NODE(<<"Messages logging engine settings">>, <<"mod_logdb_settings">>)]
    };
get_local_items(Host, [<<"mod_logdb_users">>], Server, _Lang) ->
    {result, get_all_vh_users(Host, Server)};
get_local_items(Host, [<<"mod_logdb_users">>, <<$@, Diap/binary>>], Server, Lang) ->
    Users = ejabberd_auth:get_vh_registered_users(Host),
    SUsers = lists:sort([{S, U} || {U, S} <- Users]),
    try
        [S1, S2] = ejabberd_regexp:split(Diap, <<"-">>),
        N1 = binary_to_integer(S1),
        N2 = binary_to_integer(S2),
        Sub = lists:sublist(SUsers, N1, N2 - N1 + 1),
        {result, lists:map(fun({S, U}) ->
                               ?NODE(<< U/binary, "@", S/binary >>,
                                     << (iolist_to_binary("mod_logdb_users/"))/binary, U/binary, "@", S/binary >>)
                           end, Sub)}
    catch _:_ ->
        xmpp:err_not_acceptable()
    end;
get_local_items(_Host, [<<"mod_logdb_users">>, _User], _Server, _Lang) ->
    {result, []};
get_local_items(_Host, [<<"mod_logdb_settings">>], _Server, _Lang) ->
    {result, []};
get_local_items(_Host, Item, _Server, _Lang) ->
    ?MYDEBUG("asked for items in ~p", [Item]),
    {error, xmpp:err_item_not_found()}.

-define(INFO_RESULT(Allow, Feats, Lang),
    case Allow of
      deny -> {error, xmpp:err_forbidden(<<"Denied by ACL">>, Lang)};
      allow -> {result, Feats}
    end).

get_local_features(Acc, From,
                   #jid{lserver = LServer} = _To, Node, Lang) ->
    case gen_mod:is_loaded(LServer, mod_adhoc) of
        false ->
            Acc;
        _ ->
            LNode = tokenize(Node),
            AllowUser = acl:match_rule(LServer, mod_logdb, From),
            AllowAdmin = acl:match_rule(LServer, mod_logdb_admin, From),
            case LNode of
                 [<<"mod_logdb">>] when AllowUser == allow; AllowAdmin == allow ->
                    ?INFO_RESULT(allow, [?NS_COMMANDS], Lang);
                 [<<"mod_logdb">>] ->
                    ?INFO_RESULT(deny, [?NS_COMMANDS], Lang);
                 [<<"mod_logdb_users">>] ->
                    ?INFO_RESULT(AllowAdmin, [], Lang);
                 [<<"mod_logdb_users">>, [$@ | _]] ->
                    ?INFO_RESULT(AllowAdmin, [], Lang);
                 [<<"mod_logdb_users">>, _User] ->
                    ?INFO_RESULT(AllowAdmin, [?NS_COMMANDS], Lang);
                 [<<"mod_logdb_settings">>] ->
                    ?INFO_RESULT(AllowAdmin, [?NS_COMMANDS], Lang);
                 [] ->
                    Acc;
                 _ ->
                    Acc
            end
    end.

-define(INFO_IDENTITY(Category, Type, Name, Lang),
    [#identity{category = Category, type = Type, name = ?T(Lang, Name)}]).

-define(INFO_COMMAND(Name, Lang),
    ?INFO_IDENTITY(<<"automation">>, <<"command-node">>,
               Name, Lang)).

get_local_identity(Acc, _From, _To, Node, Lang) ->
    LNode = tokenize(Node),
    case LNode of
         [<<"mod_logdb">>] ->
            ?INFO_COMMAND(<<"Messages logging engine">>, Lang);
         [<<"mod_logdb_users">>] ->
            ?INFO_COMMAND(<<"Messages logging engine users">>, Lang);
         [<<"mod_logdb_users">>, User] ->
            ?INFO_COMMAND(User, Lang);
         [<<"mod_logdb_settings">>] ->
            ?INFO_COMMAND(<<"Messages logging engine settings">>, Lang);
         _ ->
            Acc
    end.

adhoc_local_items(Acc, From,
                  #jid{lserver = LServer, server = Server} = To, Lang) ->
    % TODO: case acl:match_rule(LServer, ???, From) of
    Items = case Acc of
                {result, Its} -> Its;
                empty -> []
            end,
    Nodes = recursively_get_local_items(LServer,
                                        <<"">>, Server, Lang),
    Nodes1 = lists:filter(
               fun(#disco_item{node = Nd}) ->
                        F = get_local_features([], From, To, Nd, Lang),
                        case F of
                            {result, [?NS_COMMANDS]} -> true;
                            _ -> false
                        end
               end, Nodes),
    {result, Items ++ Nodes1}.

recursively_get_local_items(_LServer,
                            <<"mod_logdb_users">>, _Server, _Lang) ->
    [];
recursively_get_local_items(LServer,
                            Node, Server, Lang) ->
    LNode = tokenize(Node),
    Items = case get_local_items(LServer, LNode,
                                 Server, Lang) of
                {result, Res} -> Res;
                {error, _Error} -> []
            end,
    Nodes = lists:flatten(
      lists:map(
        fun(#disco_item{jid = #jid{server = S}, node = Nd} = Item) ->
                if (S /= Server) or (Nd == <<"">>) ->
                    [];
                true ->
                    [Item, recursively_get_local_items(
                            LServer, Nd, Server, Lang)]
                end
        end, Items)),
    Nodes.

-define(COMMANDS_RESULT(Allow, From, To, Request),
    case Allow of
        deny ->
            {error, xmpp:err_forbidden(<<"Denied by ACL">>, Lang)};
        allow ->
            adhoc_local_commands(From, To, Request)
    end).

adhoc_local_commands(Acc, From, #jid{lserver = LServer} = To,
                     #adhoc_command{node = Node, lang = Lang} = Request) ->
    LNode = tokenize(Node),
    AllowUser = acl:match_rule(LServer, mod_logdb, From),
    AllowAdmin = acl:match_rule(LServer, mod_logdb_admin, From),
    case LNode of
         [<<"mod_logdb">>] when AllowUser == allow; AllowAdmin == allow ->
             ?COMMANDS_RESULT(allow, From, To, Request);
         [<<"mod_logdb_users">>, <<$@, _/binary>>] when AllowAdmin == allow ->
             Acc;
         [<<"mod_logdb_users">>, _User] when AllowAdmin == allow ->
             ?COMMANDS_RESULT(allow, From, To, Request);
         [<<"mod_logdb_settings">>] when AllowAdmin == allow ->
             ?COMMANDS_RESULT(allow, From, To, Request);
         _ ->
             Acc
    end.

adhoc_local_commands(From, #jid{lserver = LServer} = _To,
                     #adhoc_command{lang = Lang,
                                    node = Node,
                                    sid = SessionID,
                                    action = Action,
                                    xdata = XData} = Request) ->
    LNode = tokenize(Node),
    %% If the "action" attribute is not present, it is
    %% understood as "execute".  If there was no <actions/>
    %% element in the first response (which there isn't in our
    %% case), "execute" and "complete" are equivalent.
    ActionIsExecute = Action == execute orelse Action == complete,
    if Action == cancel ->
            %% User cancels request
            #adhoc_command{status = canceled, lang = Lang,
                           node = Node, sid = SessionID};
       XData == undefined, ActionIsExecute ->
            %% User requests form
            case get_form(LServer, LNode, Lang) of
                {result, Form} ->
                    xmpp_util:make_adhoc_response(
                      Request,
                      #adhoc_command{status = executing,
                                     xdata = Form});
                {error, Error} ->
                    {error, Error}
            end;
       XData /= undefined, ActionIsExecute ->
            %% User returns form.
            case catch set_form(From, LServer, LNode, Lang, XData) of
                {result, Res} ->
                    xmpp_util:make_adhoc_response(
                      Request,
                      #adhoc_command{xdata = Res, status = completed});
                {'EXIT', _} -> {error, xmpp:err_bad_request()};
                {error, Error} -> {error, Error}
            end;
       true ->
            {error, xmpp:err_bad_request(<<"Unexpected action">>, Lang)}
    end.

-define(TVFIELD(Type, Var, Val),
    #xdata_field{type = Type, var = Var, values = [Val]}).

-define(HFIELD(),
    ?TVFIELD(hidden, <<"FORM_TYPE">>, (?NS_ADMIN))).

get_user_form(LUser, LServer, Lang) ->
    ?MYDEBUG("get_user_form ~p ~p", [LUser, LServer]),
    %From = jid:encode(jid:remove_resource(Jid)),
    #user_settings{dolog_default=DLD,
                   dolog_list=DLL,
                   donotlog_list=DNLL} = get_user_settings(LUser, LServer),
    Fs = [
          #xdata_field{
             type = 'list-single',
             label = ?T(Lang, <<"Default">>),
             var = <<"dolog_default">>,
             values = [misc:atom_to_binary(DLD)],
             options = [#xdata_option{label = ?T(Lang, <<"Log Messages">>),
                                      value = <<"true">>},
                        #xdata_option{label = ?T(Lang, <<"Do Not Log Messages">>),
                                      value = <<"false">>}]},
          #xdata_field{
             type = 'text-multi',
             label = ?T(Lang, <<"Log Messages">>),
             var = <<"dolog_list">>,
             values = DLL},
          #xdata_field{
             type = 'text-multi',
             label = ?T(Lang, <<"Do Not Log Messages">>),
             var = <<"donotlog_list">>,
             values = DNLL}
         ],
    {result, #xdata{
                title = ?T(Lang, <<"Messages logging engine settings">>),
                type = form,
                instructions = [<< (?T(Lang, <<"Set logging preferences">>))/binary,
                                               (iolist_to_binary(": "))/binary,
                                               LUser/binary, "@", LServer/binary >>],
                fields = [?HFIELD()|
                          Fs]}}.

get_settings_form(Host, Lang) ->
    ?MYDEBUG("get_settings_form ~p ~p", [Host, Lang]),
    #state{dbmod=_DBMod,
           dbs=_DBs,
           dolog_default=DLD,
           ignore_jids=IgnoreJids,
           groupchat=GroupChat,
           purge_older_days=PurgeDaysT,
           drop_messages_on_user_removal=MRemoval,
           poll_users_settings=PollTime} = mod_logdb:get_module_settings(Host),

    PurgeDays =
       case PurgeDaysT of
            never -> <<"never">>;
            Num when is_integer(Num) -> integer_to_binary(Num);
            _ -> <<"unknown">>
       end,
    Fs = [
          #xdata_field{
             type = 'list-single',
             label = ?T(Lang, <<"Default">>),
             var = <<"dolog_default">>,
             values = [misc:atom_to_binary(DLD)],
             options = [#xdata_option{label = ?T(Lang, <<"Log Messages">>),
                                      value = <<"true">>},
                        #xdata_option{label = ?T(Lang, <<"Do Not Log Messages">>),
                                      value = <<"false">>}]},
          #xdata_field{
             type = 'list-single',
             label = ?T(Lang, <<"Drop messages on user removal">>),
             var = <<"drop_messages_on_user_removal">>,
             values = [misc:atom_to_binary(MRemoval)],
             options = [#xdata_option{label = ?T(Lang, <<"Drop">>),
                                      value = <<"true">>},
                        #xdata_option{label = ?T(Lang, <<"Do not drop">>),
                                      value = <<"false">>}]},
          #xdata_field{
             type = 'list-single',
             label = ?T(Lang, <<"Groupchat messages logging">>),
             var = <<"groupchat">>,
             values = [misc:atom_to_binary(GroupChat)],
             options = [#xdata_option{label = ?T(Lang, <<"all">>),
                                      value = <<"all">>},
                        #xdata_option{label = ?T(Lang, <<"none">>),
                                      value = <<"none">>},
                        #xdata_option{label = ?T(Lang, <<"send">>),
                                      value = <<"send">>}]},
          #xdata_field{
             type = 'text-multi',
             label = ?T(Lang, <<"Jids/Domains to ignore">>),
             var = <<"ignore_list">>,
             values = IgnoreJids},
          #xdata_field{
             type = 'text-single',
             label = ?T(Lang, <<"Purge messages older than (days)">>),
             var = <<"purge_older_days">>,
             values = [iolist_to_binary(PurgeDays)]},
          #xdata_field{
             type = 'text-single',
             label = ?T(Lang, <<"Poll users settings (seconds)">>),
             var = <<"poll_users_settings">>,
             values = [integer_to_binary(PollTime)]}
         ],
    {result, #xdata{
                title = ?T(Lang, <<"Messages logging engine settings (run-time)">>),
                instructions = [?T(Lang, <<"Set run-time settings">>)],
                type = form,
                fields = [?HFIELD()|
                          Fs]}}.

get_form(_Host, [<<"mod_logdb_users">>, User], Lang) ->
    #jid{luser=LUser, lserver=LServer} = jid:decode(User),
    get_user_form(LUser, LServer, Lang);
get_form(Host, [<<"mod_logdb_settings">>], Lang) ->
    get_settings_form(Host, Lang);
get_form(_Host, Command, _Lang) ->
    ?MYDEBUG("asked for form ~p", [Command]),
    {error, xmpp:err_service_unavailable()}.

check_log_list([]) ->
    ok;
check_log_list([<<>>]) ->
    ok;
check_log_list([Head | Tail]) ->
    case binary:match(Head, <<$@>>) of
         nomatch -> throw(error);
         {_, _} -> ok
    end,
    % this check for Head to be valid jid
    case catch jid:decode(Head) of
         {'EXIT', _Reason} -> throw(error);
         _ -> check_log_list(Tail)
    end.

check_ignore_list([]) ->
    ok;
check_ignore_list([<<>>]) ->
    ok;
check_ignore_list([<<>> | Tail]) ->
    check_ignore_list(Tail);
check_ignore_list([Head | Tail]) ->
    case binary:match(Head, <<$@>>) of
         {_, _} -> ok;
         nomatch -> throw(error)
    end,
    Jid2Test = case Head of
                    << $@, _Rest/binary >> ->  << "a", Head/binary >>;
                    Jid -> Jid
               end,
    % this check for Head to be valid jid
    case catch jid:decode(Jid2Test) of
         {'EXIT', _Reason} -> throw(error);
         _ -> check_ignore_list(Tail)
    end.

get_value(Field, XData) -> hd(get_values(Field, XData)).

get_values(Field, XData) ->
    xmpp_util:get_xdata_values(Field, XData).

parse_users_settings(XData) ->
    DLD = case get_value(<<"dolog_default">>, XData) of
               ValueDLD when ValueDLD == <<"true">>;
                             ValueDLD == <<"false">> ->
                  list_to_bool(ValueDLD);
              _ -> throw(bad_request)
          end,

    ListDLL = get_values(<<"dolog_list">>, XData),
    DLL = case catch check_log_list(ListDLL) of
                  ok -> ListDLL;
                  error -> throw(bad_request)
             end,

    ListDNLL = get_values(<<"donotlog_list">>, XData),
    DNLL = case catch check_log_list(ListDNLL) of
                  ok -> ListDNLL;
                  error -> throw(bad_request)
             end,

    #user_settings{dolog_default=DLD,
                   dolog_list=DLL,
                   donotlog_list=DNLL}.

parse_module_settings(XData) ->
    DLD = case get_value(<<"dolog_default">>, XData) of
               ValueDLD when ValueDLD == <<"true">>;
                             ValueDLD == <<"false">> ->
                   list_to_bool(ValueDLD);
               _ -> throw(bad_request)
          end,
    MRemoval = case get_value(<<"drop_messages_on_user_removal">>, XData) of
                    ValueMRemoval when ValueMRemoval == <<"true">>;
                                       ValueMRemoval == <<"false">> ->
                        list_to_bool(ValueMRemoval);
                    _ -> throw(bad_request)
               end,
    GroupChat = case get_value(<<"groupchat">>, XData) of
                     ValueGroupChat when ValueGroupChat == <<"none">>;
                                         ValueGroupChat == <<"all">>;
                                         ValueGroupChat == <<"send">> ->
                         misc:binary_to_atom(ValueGroupChat);
                     _ -> throw(bad_request)
                end,
    ListIgnore = get_values(<<"ignore_list">>, XData),
    Ignore = case catch check_ignore_list(ListIgnore) of
                  ok -> ListIgnore;
                  error -> throw(bad_request)
             end,
    Purge = case get_value(<<"purge_older_days">>, XData) of
                 <<"never">> -> never;
                 ValuePurge ->
                    case catch binary_to_integer(ValuePurge) of
                         IntValuePurge when is_integer(IntValuePurge) -> IntValuePurge;
                         _ -> throw(bad_request)
                    end
            end,
    Poll = case catch binary_to_integer(get_value(<<"poll_users_settings">>, XData)) of
                IntValuePoll when is_integer(IntValuePoll) -> IntValuePoll;
                _ -> throw(bad_request)
           end,
    #state{dolog_default=DLD,
           groupchat=GroupChat,
           ignore_jids=Ignore,
           purge_older_days=Purge,
           drop_messages_on_user_removal=MRemoval,
           poll_users_settings=Poll}.

set_form(_From, _Host, [<<"mod_logdb_users">>, User], Lang, XData) ->
    #jid{luser=LUser, lserver=LServer} = jid:decode(User),
    Txt = "Parse user settings failed",
    case catch parse_users_settings(XData) of
         bad_request ->
            ?ERROR_MSG("Failed to set user form: bad_request", []),
            {error, xmpp:err_bad_request(Txt, Lang)};
         {'EXIT', Reason} ->
            ?ERROR_MSG("Failed to set user form ~p", [Reason]),
            {error, xmpp:err_bad_request(Txt, Lang)};
         UserSettings ->
            case mod_logdb:set_user_settings(LUser, LServer, UserSettings) of
                 ok ->
                    {result, undefined};
                 error ->
                    {error, xmpp:err_internal_server_error()}
            end
    end;
set_form(_From, Host, [<<"mod_logdb_settings">>], Lang, XData) ->
    Txt = "Parse module settings failed",
    case catch parse_module_settings(XData) of
         bad_request ->
            ?ERROR_MSG("Failed to set settings form: bad_request", []),
            {error, xmpp:err_bad_request(Txt, Lang)};
         {'EXIT', Reason} ->
            ?ERROR_MSG("Failed to set settings form ~p", [Reason]),
            {error, xmpp:err_bad_request(Txt, Lang)};
         Settings ->
            case mod_logdb:set_module_settings(Host, Settings) of
                 ok ->
                    {result, undefined};
                 error ->
                    {error, xmpp:err_internal_server_error()}
            end
    end;
set_form(From, _Host, Node, _Lang, XData) ->
    User = jid:encode(jid:remove_resource(From)),
    ?MYDEBUG("set form for ~p at ~p XData=~p", [User, Node, XData]),
    {error, xmpp:err_service_unavailable()}.

get_all_vh_users(Host, Server) ->
    case catch ejabberd_auth:get_vh_registered_users(Host) of
        {'EXIT', _Reason} ->
            [];
        Users ->
            SUsers = lists:sort([{S, U} || {U, S} <- Users]),
            case length(SUsers) of
                N when N =< 100 ->
                    lists:map(fun({S, U}) ->
                                  #disco_item{jid = jid:make(Server),
                                              node = <<"mod_logdb_users/", U/binary, $@, S/binary>>,
                                              name = << U/binary, "@", S/binary >>}
                              end, SUsers);
                N ->
                    NParts = trunc(math:sqrt(N * 6.17999999999999993783e-1)) + 1,
                    M = trunc(N / NParts) + 1,
                    lists:map(fun(K) ->
                                      L = K + M - 1,
                                      Node = <<"@",
                                               (integer_to_binary(K))/binary,
                                               "-",
                                               (integer_to_binary(L))/binary
                                             >>,
                                      {FS, FU} = lists:nth(K, SUsers),
                                      {LS, LU} =
                                          if L < N -> lists:nth(L, SUsers);
                                             true -> lists:last(SUsers)
                                          end,
                                      Name =
                                          <<FU/binary, "@", FS/binary,
                                           " -- ",
                                           LU/binary, "@", LS/binary>>,
                                      #disco_item{jid = jid:make(Host),
                                                  node = <<"mod_logdb_users/", Node/binary>>,
                                                  name = Name}
                              end, lists:seq(1, N, M))
            end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% webadmin hooks
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
webadmin_menu(Acc, _Host, Lang) ->
    [{<<"messages">>, ?T(Lang, <<"Users Messages">>)} | Acc].

webadmin_user(Acc, User, Server, Lang) ->
    Sett = get_user_settings(User, Server),
    Log =
      case Sett#user_settings.dolog_default of
           false ->
              ?INPUTT(<<"submit">>, <<"dolog">>, <<"Log Messages">>);
           true ->
              ?INPUTT(<<"submit">>, <<"donotlog">>, <<"Do Not Log Messages">>);
           _ -> []
      end,
    Acc ++ [?XE(<<"h3">>, [?ACT(<<"messages/">>, <<"Messages">>), ?C(<<" ">>), Log])].

webadmin_page(_, Host,
              #request{path = [<<"messages">>],
                       q = Query,
                       lang = Lang}) ->
    Res = vhost_messages_stats(Host, Query, Lang),
    {stop, Res};
webadmin_page(_, Host,
              #request{path = [<<"messages">>, Date],
                       q = Query,
                       lang = Lang}) ->
    Res = vhost_messages_stats_at(Host, Query, Lang, Date),
    {stop, Res};
webadmin_page(_, Host,
              #request{path = [<<"user">>, U, <<"messages">>],
                       q = Query,
                       lang = Lang}) ->
    Res = user_messages_stats(U, Host, Query, Lang),
    {stop, Res};
webadmin_page(_, Host,
              #request{path = [<<"user">>, U, <<"messages">>, Date],
                       q = Query,
                       lang = Lang}) ->
    Res = mod_logdb:user_messages_stats_at(U, Host, Query, Lang, Date),
    {stop, Res};
webadmin_page(Acc, _Host, _R) -> Acc.

user_parse_query(_, <<"dolog">>, User, Server, _Query) ->
    Sett = get_user_settings(User, Server),
    % TODO: check returned value
    set_user_settings(User, Server, Sett#user_settings{dolog_default=true}),
    {stop, ok};
user_parse_query(_, <<"donotlog">>, User, Server, _Query) ->
    Sett = get_user_settings(User, Server),
    % TODO: check returned value
    set_user_settings(User, Server, Sett#user_settings{dolog_default=false}),
    {stop, ok};
user_parse_query(Acc, _Action, _User, _Server, _Query) ->
    Acc.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% webadmin funcs
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
vhost_messages_stats(Server, Query, Lang) ->
    Res = case catch vhost_messages_parse_query(Server, Query) of
                     {'EXIT', Reason} ->
                         ?ERROR_MSG("~p", [Reason]),
                         error;
                     VResult -> VResult
          end,
    {Time, Value} = timer:tc(mod_logdb, get_vhost_stats, [Server]),
    ?INFO_MSG("get_vhost_stats(~p) elapsed ~p sec", [Server, Time/1000000]),
    %case get_vhost_stats(Server) of
    case Value of
         {'EXIT', CReason} ->
              ?ERROR_MSG("Failed to get_vhost_stats: ~p", [CReason]),
              [?XC(<<"h1">>, ?T(Lang, <<"Error occupied while fetching list">>))];
         {error, GReason} ->
              ?ERROR_MSG("Failed to get_vhost_stats: ~p", [GReason]),
              [?XC(<<"h1">>, ?T(Lang, <<"Error occupied while fetching list">>))];
         {ok, []} ->
              [?XC(<<"h1">>, list_to_binary(io_lib:format(?T(Lang, <<"No logged messages for ~s">>), [Server])))];
         {ok, Dates} ->
              Fun = fun({Date, Count}) ->
                         DateBin = iolist_to_binary(Date),
                         ID = misc:encode_base64( << Server/binary, DateBin/binary >> ),
                         ?XE(<<"tr">>,
                          [?XAE(<<"td">>, [{<<"class">>, <<"valign">>}],
                            [?INPUT(<<"checkbox">>, <<"selected">>, ID)]),
                           ?XE(<<"td">>, [?AC(DateBin, DateBin)]),
                           ?XC(<<"td">>, integer_to_binary(Count))
                          ])
                    end,

              [?XC(<<"h1">>, list_to_binary(io_lib:format(?T(Lang, <<"Logged messages for ~s">>), [Server])))] ++
               case Res of
                    ok -> [?CT(<<"Submitted">>), ?P];
                    error -> [?CT(<<"Bad format">>), ?P];
                    nothing -> []
               end ++
               [?XAE(<<"form">>, [{<<"action">>, <<"">>}, {<<"method">>, <<"post">>}],
                [?XE(<<"table">>,
                 [?XE(<<"thead">>,
                  [?XE(<<"tr">>,
                   [?X(<<"td">>),
                    ?XCT(<<"td">>, <<"Date">>),
                    ?XCT(<<"td">>, <<"Count">>)
                   ])]),
                  ?XE(<<"tbody">>,
                      lists:map(Fun, Dates)
                     )]),
                  ?BR,
                  ?INPUTT(<<"submit">>, <<"delete">>, <<"Delete Selected">>)
                ])]
   end.

vhost_messages_stats_at(Server, Query, Lang, Date) ->
   {Time, Value} = timer:tc(mod_logdb, get_vhost_stats_at, [Server, Date]),
   ?INFO_MSG("get_vhost_stats_at(~p,~p) elapsed ~p sec", [Server, Date, Time/1000000]),
   %case get_vhost_stats_at(Server, Date) of
   case Value of
        {'EXIT', CReason} ->
             ?ERROR_MSG("Failed to get_vhost_stats_at: ~p", [CReason]),
             [?XC(<<"h1">>, ?T(Lang, <<"Error occupied while fetching list">>))];
        {error, GReason} ->
             ?ERROR_MSG("Failed to get_vhost_stats_at: ~p", [GReason]),
             [?XC(<<"h1">>, ?T(Lang, <<"Error occupied while fetching list">>))];
        {ok, []} ->
             [?XC(<<"h1">>, list_to_binary(io_lib:format(?T(Lang, <<"No logged messages for ~s at ~s">>), [Server, Date])))];
        {ok, Stats} ->
             Res = case catch vhost_messages_at_parse_query(Server, Date, Stats, Query) of
                        {'EXIT', Reason} ->
                            ?ERROR_MSG("~p", [Reason]),
                            error;
                        VResult -> VResult
                   end,
             Fun = fun({User, Count}) ->
                         UserBin = iolist_to_binary(User),
                         ID = misc:encode_base64( << UserBin/binary, Server/binary >> ),
                         ?XE(<<"tr">>,
                          [?XAE(<<"td">>, [{<<"class">>, <<"valign">>}],
                            [?INPUT(<<"checkbox">>, <<"selected">>, ID)]),
                           ?XE(<<"td">>, [?AC(<< <<"../user/">>/binary, UserBin/binary, <<"/messages/">>/binary, Date/binary >>, UserBin)]),
                           ?XC(<<"td">>, integer_to_binary(Count))
                          ])
                   end,
             [?XC(<<"h1">>, list_to_binary(io_lib:format(?T(Lang, <<"Logged messages for ~s at ~s">>), [Server, Date])))] ++
              case Res of
                    ok -> [?CT(<<"Submitted">>), ?P];
                    error -> [?CT(<<"Bad format">>), ?P];
                    nothing -> []
              end ++
              [?XAE(<<"form">>, [{<<"action">>, <<"">>}, {<<"method">>, <<"post">>}],
                [?XE(<<"table">>,
                 [?XE(<<"thead">>,
                  [?XE(<<"tr">>,
                   [?X(<<"td">>),
                    ?XCT(<<"td">>, <<"User">>),
                    ?XCT(<<"td">>, <<"Count">>)
                   ])]),
                  ?XE(<<"tbody">>,
                      lists:map(Fun, Stats)
                     )]),
                  ?BR,
                  ?INPUTT(<<"submit">>, <<"delete">>, <<"Delete Selected">>)
                ])]
   end.

user_messages_stats(User, Server, Query, Lang) ->
    Jid = jid:encode({User, Server, ""}),

    Res = case catch user_messages_parse_query(User, Server, Query) of
               {'EXIT', Reason} ->
                    ?ERROR_MSG("~p", [Reason]),
                    error;
               VResult -> VResult
          end,

   {Time, Value} = timer:tc(mod_logdb, get_user_stats, [User, Server]),
   ?INFO_MSG("get_user_stats(~p,~p) elapsed ~p sec", [User, Server, Time/1000000]),

   case Value of
        {'EXIT', CReason} ->
            ?ERROR_MSG("Failed to get_user_stats: ~p", [CReason]),
            [?XC(<<"h1">>, ?T(Lang, <<"Error occupied while fetching days">>))];
        {error, GReason} ->
            ?ERROR_MSG("Failed to get_user_stats: ~p", [GReason]),
            [?XC(<<"h1">>, ?T(Lang, <<"Error occupied while fetching days">>))];
        {ok, []} ->
            [?XC(<<"h1">>, list_to_binary(io_lib:format(?T(Lang, <<"No logged messages for ~s">>), [Jid])))];
        {ok, Dates} ->
            Fun = fun({Date, Count}) ->
                      DateBin = iolist_to_binary(Date),
                      ID = misc:encode_base64( << User/binary, DateBin/binary >> ),
                      ?XE(<<"tr">>,
                       [?XAE(<<"td">>, [{<<"class">>, <<"valign">>}],
                         [?INPUT(<<"checkbox">>, <<"selected">>, ID)]),
                        ?XE(<<"td">>, [?AC(DateBin, DateBin)]),
                        ?XC(<<"td">>, iolist_to_binary(integer_to_list(Count)))
                       ])
                  end,
            [?XC(<<"h1">>, list_to_binary(io_lib:format(?T(Lang, "Logged messages for ~s"), [Jid])))] ++
             case Res of
                   ok -> [?CT(<<"Submitted">>), ?P];
                   error -> [?CT(<<"Bad format">>), ?P];
                   nothing -> []
             end ++
             [?XAE(<<"form">>, [{<<"action">>, <<"">>}, {<<"method">>, <<"post">>}],
              [?XE(<<"table">>,
               [?XE(<<"thead">>,
                [?XE(<<"tr">>,
                 [?X(<<"td">>),
                  ?XCT(<<"td">>, <<"Date">>),
                  ?XCT(<<"td">>, <<"Count">>)
                 ])]),
                ?XE(<<"tbody">>,
                    lists:map(Fun, Dates)
                   )]),
                ?BR,
                ?INPUTT(<<"submit">>, <<"delete">>, <<"Delete Selected">>)
              ])]
    end.

search_user_nick(User, List) ->
    case lists:keysearch(User, 1, List) of
         {value,{User, []}} ->
           nothing;
         {value,{User, Nick}} ->
           Nick;
         false ->
           nothing
    end.

user_messages_stats_at(User, Server, Query, Lang, Date) ->
   Jid = jid:encode({User, Server, ""}),

   {Time, Value} = timer:tc(mod_logdb, get_user_messages_at, [User, Server, Date]),
   ?INFO_MSG("get_user_messages_at(~p,~p,~p) elapsed ~p sec", [User, Server, Date, Time/1000000]),
   case Value of
        {'EXIT', CReason} ->
           ?ERROR_MSG("Failed to get_user_messages_at: ~p", [CReason]),
           [?XC(<<"h1">>, ?T(Lang, <<"Error occupied while fetching messages">>))];
        {error, GReason} ->
           ?ERROR_MSG("Failed to get_user_messages_at: ~p", [GReason]),
           [?XC(<<"h1">>, ?T(Lang, <<"Error occupied while fetching messages">>))];
        {ok, []} ->
           [?XC(<<"h1">>, list_to_binary(io_lib:format(?T(Lang, <<"No logged messages for ~s at ~s">>), [Jid, Date])))];
        {ok, User_messages} ->
           Res =  case catch user_messages_at_parse_query(Server,
                                                          Date,
                                                          User_messages,
                                                          Query) of
                       {'EXIT', Reason} ->
                            ?ERROR_MSG("~p", [Reason]),
                            error;
                       VResult -> VResult
                  end,

           UR = ejabberd_hooks:run_fold(roster_get, Server, [], [{User, Server}]),
           UserRoster =
                 lists:map(fun(Item) ->
                              {jid:encode(Item#roster.jid), Item#roster.name}
                          end, UR),

           UniqUsers = lists:foldl(fun(#msg{peer_name=PName, peer_server=PServer}, List) ->
                                 ToAdd = PName++"@"++PServer,
                                 case lists:member(ToAdd, List) of
                                      true -> List;
                                      false -> lists:append([ToAdd], List)
                                 end
                               end, [], User_messages),

           % Users to filter (sublist of UniqUsers)
           CheckedUsers = case lists:keysearch(<<"filter">>, 1, Query) of
                           {value, _} ->
                              lists:filter(fun(UFUser) ->
                                                ID = misc:encode_base64(term_to_binary(UFUser)),
                                                lists:member({<<"selected">>, ID}, Query)
                                           end, UniqUsers);
                           false -> []
                         end,

           % UniqUsers in html (noone selected -> everyone selected)
           Users = lists:map(fun(UHUser) ->
                                ID = misc:encode_base64(term_to_binary(UHUser)),
                                Input = case lists:member(UHUser, CheckedUsers) of
                                         true -> [?INPUTC(<<"checkbox">>, <<"selected">>, ID)];
                                         false when CheckedUsers == [] -> [?INPUTC(<<"checkbox">>, <<"selected">>, ID)];
                                         false -> [?INPUT(<<"checkbox">>, <<"selected">>, ID)]
                                        end,
                                Nick =
                                   case search_user_nick(UHUser, UserRoster) of
                                        nothing -> <<"">>;
                                        N -> iolist_to_binary( " ("++ N ++")" )
                                   end,
                                ?XE(<<"tr">>,
                                 [?XE(<<"td">>, Input),
                                  ?XC(<<"td">>, iolist_to_binary(UHUser++Nick))])
                             end, lists:sort(UniqUsers)),
           % Messages to show (based on Users)
           User_messages_filtered = case CheckedUsers of
                                         [] -> User_messages;
                                         _  -> lists:filter(fun(#msg{peer_name=PName, peer_server=PServer}) ->
                                                  lists:member(PName++"@"++PServer, CheckedUsers)
                                               end, User_messages)
                                    end,

           Msgs_Fun = fun(#msg{timestamp=Timestamp,
                               subject=Subject,
                               direction=Direction,
                               peer_name=PName, peer_server=PServer, peer_resource=PRes,
                               type=Type,
                               body=Body}) ->
                      Text = case Subject of
                                  "" -> iolist_to_binary(Body);
                                  _ -> iolist_to_binary([binary_to_list(?T(Lang, <<"Subject">>)) ++ ": " ++ Subject ++ "\n" ++ Body])
                             end,
                      Resource = case PRes of
                                      [] -> [];
                                      undefined -> [];
                                      R -> "/" ++ R
                                 end,
                      UserNick =
                         case search_user_nick(PName++"@"++PServer, UserRoster) of
                              nothing when PServer == Server ->
                                   PName;
                              nothing when Type == "groupchat", Direction == from ->
                                   PName++"@"++PServer++Resource;
                              nothing ->
                                   PName++"@"++PServer;
                              N -> N
                         end,
                      ID = misc:encode_base64(term_to_binary(Timestamp)),
                      ?XE(<<"tr">>,
                       [?XE(<<"td">>, [?INPUT(<<"checkbox">>, <<"selected">>, ID)]),
                        ?XC(<<"td">>, iolist_to_binary(convert_timestamp(Timestamp))),
                        ?XC(<<"td">>, iolist_to_binary(atom_to_list(Direction)++": "++UserNick)),
                        ?XE(<<"td">>, [?XC(<<"pre">>, Text)])])
                 end,
           % Filtered user messages in html
           Msgs = lists:map(Msgs_Fun, lists:sort(User_messages_filtered)),

           [?XC(<<"h1">>, list_to_binary(io_lib:format(?T(Lang, <<"Logged messages for ~s at ~s">>), [Jid, Date])))] ++
            case Res of
                 ok -> [?CT(<<"Submitted">>), ?P];
                 error -> [?CT(<<"Bad format">>), ?P];
                 nothing -> []
            end ++
            [?XAE(<<"form">>, [{<<"action">>, <<"">>}, {<<"method">>, <<"post">>}],
             [?XE(<<"table">>,
                  [?XE(<<"thead">>,
                       [?X(<<"td">>),
                        ?XCT(<<"td">>, <<"User">>)
                       ]
                      ),
                   ?XE(<<"tbody">>,
                        Users
                      )]),
              ?INPUTT(<<"submit">>, <<"filter">>, <<"Filter Selected">>)
             ] ++
             [?XE(<<"table">>,
                  [?XE(<<"thead">>,
                       [?XE(<<"tr">>,
                        [?X(<<"td">>),
                         ?XCT(<<"td">>, <<"Date, Time">>),
                         ?XCT(<<"td">>, <<"Direction: Jid">>),
                         ?XCT(<<"td">>, <<"Body">>)
                        ])]),
                   ?XE(<<"tbody">>,
                        Msgs
                      )]),
              ?INPUTT(<<"submit">>, <<"delete">>, <<"Delete Selected">>),
              ?BR
             ]
            )]
    end.
