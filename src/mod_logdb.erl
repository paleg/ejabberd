%%%----------------------------------------------------------------------
%%% File    : mod_logdb.erl
%%% Author  : Oleg Palij (mailto,xmpp:o.palij@gmail.com)
%%% Purpose : Frontend for log user messages to db
%%% Version : trunk
%%% Id      : $Id: mod_logdb.erl 1360 2009-07-30 06:00:14Z malik $
%%% Url     : http://www.dp.uz.gov.ua/o.palij/mod_logdb/
%%%----------------------------------------------------------------------

-module(mod_logdb).
-author('o.palij@gmail.com').

-behaviour(gen_server).
-behaviour(gen_mod).

% supervisor
-export([start_link/2]).
% gen_mod
-export([start/2,stop/1]).
% gen_server
-export([code_change/3,handle_call/3,handle_cast/2,handle_info/2,init/1,terminate/2]).
% hooks
-export([send_packet/3, receive_packet/4, remove_user/2]).
-export([get_local_identity/5,
         get_local_features/5, 
         get_local_items/5,
         adhoc_local_items/4,
         adhoc_local_commands/4
%         get_sm_identity/5,
%         get_sm_features/5,
%         get_sm_items/5,
%         adhoc_sm_items/4,
%         adhoc_sm_commands/4]).
        ]).
% ejabberdctl
-export([rebuild_stats/3,
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
-include("ejabberd.hrl").
-include("mod_roster.hrl").
-include("jlib.hrl").
-include("ejabberd_ctl.hrl").
-include("adhoc.hrl").
-include("web/ejabberd_web_admin.hrl").
-include("web/ejabberd_http.hrl").

-define(PROCNAME, ejabberd_mod_logdb).
% gen_server call timeout
-define(CALL_TIMEOUT, 10000).

-record(state, {vhost, dbmod, backendPid, monref, purgeRef, pollRef, dbopts, dbs, dolog_default, ignore_jids, groupchat, purge_older_days, poll_users_settings, drop_messages_on_user_removal}).

ets_settings_table(VHost) -> list_to_atom("ets_logdb_settings_" ++ VHost).

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
    supervisor:start_child(ejabberd_sup, ChildSpec).

% supervisor starts gen_server
start_link(VHost, Opts) ->
    Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
    {ok, Pid} = gen_server:start_link({local, Proc}, ?MODULE, [VHost, Opts], []),
    Pid ! start,
    {ok, Pid}.

init([VHost, Opts]) ->
    ?MYDEBUG("Starting mod_logdb", []),
    process_flag(trap_exit, true),
    DBs = gen_mod:get_opt(dbs, Opts, [{mnesia, []}]),
    VHostDB = gen_mod:get_opt(vhosts, Opts, [{VHost, mnesia}]),
    % 10 is default becouse of using in clustered environment
    PollUsersSettings = gen_mod:get_opt(poll_users_settings, Opts, 10),

    {value,{_, DBName}} = lists:keysearch(VHost, 1, VHostDB),
    {value, {DBName, DBOpts}} = lists:keysearch(DBName, 1, DBs),

    ?MYDEBUG("Starting mod_logdb for ~p with ~p backend", [VHost, DBName]),

    DBMod = list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(DBName)),

    {ok, #state{vhost=VHost,
                dbmod=DBMod,
                dbopts=DBOpts,
                % dbs used for convert messages from one backend to other
                dbs=DBs,
                dolog_default=gen_mod:get_opt(dolog_default, Opts, true),
                drop_messages_on_user_removal=gen_mod:get_opt(drop_messages_on_user_removal, Opts, true),
                ignore_jids=gen_mod:get_opt(ignore_jids, Opts, []),
                groupchat=gen_mod:get_opt(groupchat, Opts, none),
                purge_older_days=gen_mod:get_opt(purge_older_days, Opts, never),
                poll_users_settings=PollUsersSettings}}.

cleanup(#state{vhost=VHost} = _State) ->
    ?MYDEBUG("Stopping ~s for ~p", [?MODULE, VHost]),

    %ets:delete(ets_settings_table(VHost)),

    ejabberd_hooks:delete(remove_user, VHost, ?MODULE, remove_user, 90),
    ejabberd_hooks:delete(user_send_packet, VHost, ?MODULE, send_packet, 90),
    ejabberd_hooks:delete(user_receive_packet, VHost, ?MODULE, receive_packet, 90),
    %ejabberd_hooks:delete(adhoc_sm_commands, VHost, ?MODULE, adhoc_sm_commands, 110),
    %ejabberd_hooks:delete(adhoc_sm_items, VHost, ?MODULE, adhoc_sm_items, 110),
    ejabberd_hooks:delete(adhoc_local_commands, VHost, ?MODULE, adhoc_local_commands, 110),
    ejabberd_hooks:delete(adhoc_local_items, VHost, ?MODULE, adhoc_local_items, 110),
    %ejabberd_hooks:delete(disco_sm_identity, VHost, ?MODULE, get_sm_identity, 110),
    %ejabberd_hooks:delete(disco_sm_features, VHost, ?MODULE, get_sm_features, 110),
    %ejabberd_hooks:delete(disco_sm_items, VHost, ?MODULE, get_sm_items, 110),
    ejabberd_hooks:delete(disco_local_identity, VHost, ?MODULE, get_local_identity, 110),
    ejabberd_hooks:delete(disco_local_features, VHost, ?MODULE, get_local_features, 110),
    ejabberd_hooks:delete(disco_local_items, VHost, ?MODULE, get_local_items, 110),

    ejabberd_hooks:delete(webadmin_menu_host, VHost, ?MODULE, webadmin_menu, 70),
    ejabberd_hooks:delete(webadmin_user, VHost, ?MODULE, webadmin_user, 50),
    ejabberd_hooks:delete(webadmin_page_host, VHost, ?MODULE, webadmin_page, 50),
    ejabberd_hooks:delete(webadmin_user_parse_query, VHost, ?MODULE, user_parse_query, 50),

    ?MYDEBUG("Removed hooks for ~p", [VHost]),

    %ejabberd_ctl:unregister_commands(VHost, [{"rebuild_stats", "rebuild mod_logdb module stats for vhost"}], ?MODULE, rebuild_stats),
    %Supported_backends = lists:flatmap(fun({Backend, _Opts}) ->
    %                                        [atom_to_list(Backend), " "]
    %                                   end, State#state.dbs),
    %ejabberd_ctl:unregister_commands(
    %                       VHost,
    %                       [{"copy_messages backend", "copy messages from backend to current backend. backends could be: " ++ Supported_backends }],
    %                       ?MODULE, copy_messages_ctl),
    ?MYDEBUG("Unregistered commands for ~p", [VHost]).

stop(VHost) ->
    Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
    %gen_server:call(Proc, {cleanup}),
    %?MYDEBUG("Cleanup in stop finished!!!!", []),
    %timer:sleep(10000),
    ok = supervisor:terminate_child(ejabberd_sup, Proc),
    ok = supervisor:delete_child(ejabberd_sup, Proc).

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
    Reply = DBMod:delete_messages_by_user_at(VHost, PMsgs, Date),
    {reply, Reply, State};
handle_call({delete_all_messages_by_user_at, User, Date}, _From, #state{dbmod=DBMod, vhost=VHost}=State) ->
    Reply = DBMod:delete_all_messages_by_user_at(User, VHost, Date),
    {reply, Reply, State};
handle_call({delete_messages_at, Date}, _From, #state{dbmod=DBMod, vhost=VHost}=State) ->
    Reply = DBMod:delete_messages_at(VHost, Date),
    {reply, Reply, State};
handle_call({get_vhost_stats}, _From, #state{dbmod=DBMod, vhost=VHost}=State) ->
    Reply = DBMod:get_vhost_stats(VHost),
    {reply, Reply, State};
handle_call({get_vhost_stats_at, Date}, _From, #state{dbmod=DBMod, vhost=VHost}=State) ->
    Reply = DBMod:get_vhost_stats_at(VHost, Date),
    {reply, Reply, State};
handle_call({get_user_stats, User}, _From, #state{dbmod=DBMod, vhost=VHost}=State) ->
    Reply = DBMod:get_user_stats(User, VHost),
    {reply, Reply, State};
handle_call({get_user_messages_at, User, Date}, _From, #state{dbmod=DBMod, vhost=VHost}=State) ->
    Reply = DBMod:get_user_messages_at(User, VHost, Date),
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
                ?MYDEBUG("Settings is equal", []),
                ok;
            _ ->
                case DBMod:set_user_settings(User, VHost, Set) of
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
           DBMod:drop_user(User, VHost),
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
    spawn(?MODULE, copy_messages, [[State, Backend]]),
    {noreply, State};
handle_cast({copy_messages, Backend, Date}, State) ->
    spawn(?MODULE, copy_messages, [[State, Backend, Date]]),
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
           {ok, DoLog} = DBMod:get_users_settings(VHost),
           ets:insert(ets_settings_table(VHost), DoLog),

           TrefPurge = set_purge_timer(State#state.purge_older_days),
           TrefPoll = set_poll_timer(State#state.poll_users_settings),

           ejabberd_hooks:add(remove_user, VHost, ?MODULE, remove_user, 90),
           ejabberd_hooks:add(user_send_packet, VHost, ?MODULE, send_packet, 90),
           ejabberd_hooks:add(user_receive_packet, VHost, ?MODULE, receive_packet, 90),

           ejabberd_hooks:add(disco_local_items, VHost, ?MODULE, get_local_items, 110),
           ejabberd_hooks:add(disco_local_features, VHost, ?MODULE, get_local_features, 110),
           ejabberd_hooks:add(disco_local_identity, VHost, ?MODULE, get_local_identity, 110),
           %ejabberd_hooks:add(disco_sm_items, VHost, ?MODULE, get_sm_items, 110),
           %ejabberd_hooks:add(disco_sm_features, VHost, ?MODULE, get_sm_features, 110),
           %ejabberd_hooks:add(disco_sm_identity, VHost, ?MODULE, get_sm_identity, 110),
           ejabberd_hooks:add(adhoc_local_items, VHost, ?MODULE, adhoc_local_items, 110),
           ejabberd_hooks:add(adhoc_local_commands, VHost, ?MODULE, adhoc_local_commands, 110),
           %ejabberd_hooks:add(adhoc_sm_items, VHost, ?MODULE, adhoc_sm_items, 110),
           %ejabberd_hooks:add(adhoc_sm_commands, VHost, ?MODULE, adhoc_sm_commands, 110),

           ejabberd_hooks:add(webadmin_menu_host, VHost, ?MODULE, webadmin_menu, 70),
           ejabberd_hooks:add(webadmin_user, VHost, ?MODULE, webadmin_user, 50),
           ejabberd_hooks:add(webadmin_page_host, VHost, ?MODULE, webadmin_page, 50),
           ejabberd_hooks:add(webadmin_user_parse_query, VHost, ?MODULE, user_parse_query, 50),

           ?MYDEBUG("Added hooks for ~p", [VHost]),

           %ejabberd_ctl:register_commands(
           %                VHost,
           %                [{"rebuild_stats", "rebuild mod_logdb module stats for vhost"}],
           %                ?MODULE, rebuild_stats),
           %Supported_backends = lists:flatmap(fun({Backend, _Opts}) ->
           %                                       [atom_to_list(Backend), " "]
           %                                   end, State#state.dbs),
           %ejabberd_ctl:register_commands(
           %                VHost,
           %                [{"copy_messages backend", "copy messages from backend to current backend. backends could be: " ++ Supported_backends }],
           %                ?MODULE, copy_messages_ctl),
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
send_packet(Owner, Peer, P) ->
    VHost = Owner#jid.lserver,
    Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
    gen_server:cast(Proc, {addlog, to, Owner, Peer, P}).

receive_packet(_JID, Peer, Owner, P) -> 
    VHost = Owner#jid.lserver,
    Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
    gen_server:cast(Proc, {addlog, from, Owner, Peer, P}).

remove_user(User, Server) ->
    LUser = jlib:nodeprep(User),
    LServer = jlib:nameprep(Server),
    Proc = gen_mod:get_module_proc(LServer, ?PROCNAME),
    gen_server:cast(Proc, {remove_user, LUser}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ejabberdctl
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
rebuild_stats(_Val, VHost, ["rebuild_stats"]) ->
    Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
    gen_server:cast(Proc, {rebuild_stats}),
    {stop, ?STATUS_SUCCESS};
rebuild_stats(Val, _VHost, _Args) ->
    Val.

copy_messages_ctl(_Val, VHost, ["copy_messages", Backend]) ->
    Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
    gen_server:cast(Proc, {copy_messages, Backend}),
    {stop, ?STATUS_SUCCESS};
copy_messages_ctl(_Val, VHost, ["copy_messages", Backend, Date]) ->
    Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
    gen_server:cast(Proc, {copy_messages, Backend, Date}),
    {stop, ?STATUS_SUCCESS};
copy_messages_ctl(Val, _VHost, _Args) ->
    Val.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% misc operations
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% handle_cast({addlog, E}, _)
% raw packet -> #msg
packet_parse(Owner, Peer, Packet, Direction, State) ->
    case xml:get_subtag(Packet, "body") of
         false ->
           ignore;
         Body_xml ->
           Message_type =
              case xml:get_tag_attr_s("type", Packet) of
                   [] -> "normal";
                   MType -> MType
              end,

           case Message_type of
                "groupchat" when State#state.groupchat == send, Direction == to ->
                   ok;
                "groupchat" when State#state.groupchat == send, Direction == from ->
                   throw(ignore);
                "groupchat" when State#state.groupchat == half ->
                   Rooms = ets:match(muc_online_room, '$1'),
                   Ni=lists:foldl(fun([{muc_online_room, {GName, GHost}, Pid}], Names) ->
                                   case gen_fsm:sync_send_all_state_event(Pid, {get_jid_nick,Owner}) of
                                        [] -> Names;
                                        Nick -> 
                                           lists:append(Names, [jlib:jid_to_string({GName, GHost, Nick})])
                                   end
                                  end, [], Rooms),
                   case lists:member(jlib:jid_to_string(Peer), Ni) of
                        true when Direction == from ->
                          throw(ignore);
                        _ ->
                          ok
                   end;
                "groupchat" when State#state.groupchat == none ->
                   throw(ignore);
                _ ->
                   ok
           end,

           Message_body = xml:get_tag_cdata(Body_xml),
           Message_subject =
              case xml:get_subtag(Packet, "subject") of
                   false ->
                     "";
                   Subject_xml ->
                     xml:get_tag_cdata(Subject_xml)
              end,

           OwnerName = stringprep:tolower(Owner#jid.user),
           PName = stringprep:tolower(Peer#jid.user),
           PServer = stringprep:tolower(Peer#jid.server),
           PResource = Peer#jid.resource,

           #msg{timestamp=get_timestamp(),
                owner_name=OwnerName,
                peer_name=PName,
                peer_server=PServer,
                peer_resource=PResource,
                direction=Direction,
                type=Message_type,
                subject=Message_subject,
                body=Message_body}
    end.

% called from handle_cast({addlog, _}, _) -> true (log messages) | false (do not log messages)
filter(Owner, Peer, State) ->
    OwnerStr = Owner#jid.luser++"@"++Owner#jid.lserver,
    OwnerServ = "@"++Owner#jid.lserver,
    PeerStr = Peer#jid.luser++"@"++Peer#jid.lserver,
    PeerServ = "@"++Peer#jid.lserver,

    LogTo = case ets:match_object(ets_settings_table(State#state.vhost),
                                  #user_settings{owner_name=Owner#jid.luser, _='_'}) of
                 [#user_settings{dolog_default=Default,
                                 dolog_list=DLL,
                                 donotlog_list=DNLL}] ->
                      A = lists:member(PeerStr, DLL),
                      B = lists:member(PeerStr, DNLL),
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
              [not lists:member(OwnerStr, State#state.ignore_jids),
               not lists:member(PeerStr, State#state.ignore_jids),
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
                    {ok, [Year, Month, Day]} = regexp:split(Date, "[^0-9]+"),
                    DateInSec = calendar:datetime_to_gregorian_seconds({{list_to_integer(Year), list_to_integer(Month), list_to_integer(Day)}, {0,0,1}}),
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
                 {ok, [Year, Month, Day]} = regexp:split(TableName, "[^0-9]+"),
                 { calendar:datetime_to_gregorian_seconds({{list_to_integer(Year), list_to_integer(Month), list_to_integer(Day)}, {0,0,1}}), Count }
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
    case lists:keysearch("delete", 1, Query) of
         {value, _} ->
             PMsgs = lists:filter(
                              fun(Msg) ->
                                   ID = jlib:encode_base64(binary_to_list(term_to_binary(Msg#msg.timestamp))),
                                   lists:member({"selected", ID}, Query)
                              end, Msgs),
             Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
             gen_server:call(Proc, {delete_messages_by_user_at, PMsgs, Date}, ?CALL_TIMEOUT);
         false ->
             nothing
    end.

user_messages_parse_query(User, VHost, Query) ->
    case lists:keysearch("delete", 1, Query) of
         {value, _} ->
             Dates = get_dates(VHost),
             PDates = lists:filter(
                              fun(Date) ->
                                   ID = jlib:encode_base64(binary_to_list(term_to_binary(User++Date))),
                                   lists:member({"selected", ID}, Query)
                              end, Dates),
             Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
             Rez = lists:foldl(
                          fun(Date, Acc) ->
                              lists:append(Acc,
                                           [gen_server:call(Proc,
                                                            {delete_all_messages_by_user_at, User, Date},
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
    case lists:keysearch("delete", 1, Query) of
         {value, _} ->
             Dates = get_dates(VHost),
             PDates = lists:filter(
                              fun(Date) ->
                                   ID = jlib:encode_base64(binary_to_list(term_to_binary(VHost++Date))),
                                   lists:member({"selected", ID}, Query)
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
    case lists:keysearch("delete", 1, Query) of
         {value, _} ->
             PStats = lists:filter(
                              fun({User, _Count}) ->
                                   ID = jlib:encode_base64(binary_to_list(term_to_binary(User++VHost))),
                                   lists:member({"selected", ID}, Query)
                              end, Stats),
             Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
             Rez = lists:foldl(fun({User, _Count}, Acc) ->
                                   lists:append(Acc, [gen_server:call(Proc,
                                                                      {delete_all_messages_by_user_at,
                                                                       User, Date},
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

copy_messages([#state{vhost=VHost}=State, From]) ->
    ?INFO_MSG("Going to copy messages from ~p for ~p", [From, VHost]),

    {FromDBName, FromDBOpts} =
         case lists:keysearch(list_to_atom(From), 1, State#state.dbs) of
              {value, {FN, FO}} ->
                 {FN, FO};
              false ->
                 ?ERROR_MSG("Failed to find record for ~p in dbs", [From]),
                 throw(error)
         end,

    FromDBMod = list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(FromDBName)),

    {ok, _FromPid} = FromDBMod:start(VHost, FromDBOpts),
 
    Dates = FromDBMod:get_dates(VHost),
    DatesLength = length(Dates),

    lists:foldl(fun(Date, Acc) ->
                   case copy_messages_int([FromDBMod, State#state.dbmod, VHost, Date]) of
                        ok ->
                          ?INFO_MSG("Copied messages at ~p (~p/~p)", [Date, Acc, DatesLength]);
                        Value ->
                          ?ERROR_MSG("Failed to copy messages at ~p (~p/~p): ~p", [Date, Acc, DatesLength, Value]),
                          FromDBMod:stop(VHost),
                          throw(error)
                   end,
                   Acc + 1
                  end, 1, Dates),
    ?INFO_MSG("Copied messages from ~p",  [From]),
    FromDBMod:stop(VHost);
copy_messages([#state{vhost=VHost}=State, From, Date]) ->
    {value, {FromDBName, FromDBOpts}} = lists:keysearch(list_to_atom(From), 1, State#state.dbs),
    FromDBMod = list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(FromDBName)),
    {ok, _FromPid} = FromDBMod:start(VHost, FromDBOpts),
    case catch copy_messages_int([FromDBMod, State#state.dbmod, VHost, Date]) of
         {'exit', Reason} ->
           ?ERROR_MSG("Failed to copy messages at ~p: ~p", [Date, Reason]);
         ok ->
           ?INFO_MSG("Copied messages at ~p", [Date]);
         Value ->
           ?ERROR_MSG("Failed to copy messages at ~p: ~p", [Date, Value])
    end,
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
    ToStats = case mod_logdb:get_vhost_stats_at(VHost, Date) of
                   {ok, Stats} -> Stats;
                   {error, _} -> []
              end,

    FromStatsS = lists:keysort(1, FromStats),
    ToStatsS = lists:keysort(1, ToStats),

    StatsLength = length(FromStats),

    CopyFun = if
                                                   % destination table is empty
                FromDBMod /= mod_logdb_mnesia_old, ToStats == [] ->
                    fun({User, _Count}, Acc) ->
                        {ok, Msgs} = FromDBMod:get_user_messages_at(User, VHost, Date),
                        MAcc =
                          lists:foldl(fun(Msg, MFAcc) ->
                                          ok = ToDBMod:log_message(VHost, Msg),
                                          MFAcc + 1
                                      end, 0, Msgs),
                        NewAcc = Acc + 1,
                        ?INFO_MSG("Copied ~p messages for ~p (~p/~p) at ~p", [MAcc, User, NewAcc, StatsLength, Date]),
                        %timer:sleep(100),
                        NewAcc
                    end;
                                                   % destination table is not empty
                FromDBMod /= mod_logdb_mnesia_old, ToStats /= [] ->
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
                                                  ok = ToDBMod:log_message(VHost, Msg),
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
                    end;
                % copying from mod_logmnesia
                true ->
                    fun({User, _Count}, Acc) ->
                        ToStats =
                           case ToDBMod:get_user_messages_at(User, VHost, Date) of
                                {ok, []} ->
                                  ok;
                                {ok, ToMsgs} ->
                                  lists:foreach(fun(#msg{timestamp=Tst}) when length(Tst) == 16 ->
                                                     ets:insert(mod_logdb_temp, {Tst});
                                                   % mysql, pgsql removes final zeros after decimal point
                                                   (#msg{timestamp=Tst}) when length(Tst) < 15 ->
                                                     {F, _} = string:to_float(Tst++".0"),
                                                     [T] = io_lib:format("~.5f", [F]),
                                                     ets:insert(mod_logdb_temp, {T})
                                                end, ToMsgs);
                                {error, _} ->
                                  ok
                           end,
                        {ok, Msgs} = FromDBMod:get_user_messages_at(User, VHost, Date),

                        MAcc =
                          lists:foldl(
                            fun({msg, TU, TS, TR, FU, FS, FR, Type, Subj, Body, Timest},
                                MFAcc) ->
                                  [Timestamp] = if is_float(Timest) == true ->
                                                     io_lib:format("~.5f", [Timest]);
                                                   % early versions of mod_logmnesia
                                                   is_integer(Timest) == true ->
                                                     io_lib:format("~.5f", [Timest-719528*86400.0]);
                                                   true ->
                                                     ?ERROR_MSG("Incorrect timestamp ~p", [Timest]),
                                                     throw(error)
                                                end,
                                  case ets:member(mod_logdb_temp, Timestamp) of
                                       false ->
                                          if
                                           % from
                                           TS == VHost ->
                                             TMsg = #msg{timestamp=Timestamp,
                                                         owner_name=TU,
                                                         peer_name=FU, peer_server=FS, peer_resource=FR,
                                                         direction=from,
                                                         type=Type,
                                                         subject=Subj, body=Body},
                                             ok = ToDBMod:log_message(VHost, TMsg);
                                           true -> ok
                                         end,
                                         if
                                           % to
                                           FS == VHost ->
                                             FMsg = #msg{timestamp=Timestamp,
                                                         owner_name=FU,
                                                         peer_name=TU, peer_server=TS, peer_resource=TR,
                                                         direction=to,
                                                         type=Type,
                                                         subject=Subj, body=Body},
                                             ok = ToDBMod:log_message(VHost, FMsg);
                                           true -> ok
                                         end,
                                         ets:insert(mod_logdb_temp, {Timestamp}),
                                         MFAcc + 1;
                                       true -> % not ets:member
                                          MFAcc
                                   end % case
                          end, 0, Msgs), % foldl
                        NewAcc = Acc + 1,
                        ?INFO_MSG("Copied ~p messages for ~p (~p/~p) at ~p", [MAcc, User, NewAcc, StatsLength, Date]),
                        %timer:sleep(100),
                        NewAcc
                    end % fun
              end, % if FromDBMod /= mod_logdb_mnesia_old

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

list_to_bool(Num) ->
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
    Str = lists:flatmap(fun(Elm) -> Elm ++ "\n" end, List),
    lists:sublist(Str, length(Str)-1).

string_to_list(null) ->
    [];
string_to_list([]) ->
    [];
string_to_list(String) ->
    {ok, List} = regexp:split(String, "\n"),
    List.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% ad-hoc (copy/pasted from mod_configure.erl)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-define(ITEMS_RESULT(Allow, LNode, Fallback),
    case Allow of
        deny ->
            Fallback;
        allow ->
            case get_local_items(LServer, LNode,
                                 jlib:jid_to_string(To), Lang) of
                {result, Res} ->
                    {result, Res};
                {error, Error} ->
                    {error, Error}
            end
    end).

get_local_items(Acc, From, #jid{lserver = LServer} = To, "", Lang) ->
    case gen_mod:is_loaded(LServer, mod_adhoc) of
        false ->
            Acc;
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
                                     jlib:jid_to_string(To), Lang) of
                     {result, Res} ->
                        {result, Items ++ Res};
                     {error, _Error} ->
                        {result, Items}
                end;
              true ->
                {result, Items}
            end
    end;
get_local_items(Acc, From, #jid{lserver = LServer} = To, Node, Lang) ->
    case gen_mod:is_loaded(LServer, mod_adhoc) of
        false ->
            Acc;
        _ ->
            LNode = string:tokens(Node, "/"),
            AllowAdmin = acl:match_rule(LServer, mod_logdb_admin, From),
            case LNode of
                 ["mod_logdb"] ->
                      ?ITEMS_RESULT(AllowAdmin, LNode, {error, ?ERR_FORBIDDEN});
                 ["mod_logdb_users"] ->
                      ?ITEMS_RESULT(AllowAdmin, LNode, {error, ?ERR_FORBIDDEN});
                 ["mod_logdb_users", [$@ | _]] ->
                      ?ITEMS_RESULT(AllowAdmin, LNode, {error, ?ERR_FORBIDDEN});
                 ["mod_logdb_users", _User] ->
                      ?ITEMS_RESULT(AllowAdmin, LNode, {error, ?ERR_FORBIDDEN});
                 ["mod_logdb_settings"] ->
                      ?ITEMS_RESULT(AllowAdmin, LNode, {error, ?ERR_FORBIDDEN});
                 _ ->
                      Acc
            end
    end.

-define(NODE(Name, Node),
        {xmlelement, "item",
         [{"jid", Server},
          {"name", translate:translate(Lang, Name)},
          {"node", Node}], []}).

get_local_items(_Host, [], Server, Lang) ->
    {result,
     [?NODE("Messages logging engine", "mod_logdb")]
    };
get_local_items(_Host, ["mod_logdb"], Server, Lang) ->
    {result,
     [?NODE("Messages logging engine users", "mod_logdb_users"),
      ?NODE("Messages logging engine settings", "mod_logdb_settings")]
    };
get_local_items(Host, ["mod_logdb_users"], Server, Lang) ->
    {result, get_all_vh_users(Host, Server, Lang)};
get_local_items(_Host, ["mod_logdb_users", [$@ | Diap]], Server, Lang) ->
    case catch ejabberd_auth:dirty_get_registered_users() of
        {'EXIT', _Reason} ->
            ?ERR_INTERNAL_SERVER_ERROR;
        Users ->
            SUsers = lists:sort([{S, U} || {U, S} <- Users]),
            case catch begin
                           {ok, [S1, S2]} = regexp:split(Diap, "-"),
                           N1 = list_to_integer(S1),
                           N2 = list_to_integer(S2),
                           Sub = lists:sublist(SUsers, N1, N2 - N1 + 1),
                           lists:map(fun({S, U}) ->
                                      ?NODE(U ++ "@" ++ S, "mod_logdb_users/" ++ U ++ "@" ++ S)
                                     end, Sub)
                       end of
                {'EXIT', _Reason} ->
                    ?ERR_NOT_ACCEPTABLE;
                Res ->
                    {result, Res}
            end
    end;
get_local_items(_Host, ["mod_logdb_users", _User], _Server, _Lang) ->
    {result, []};
get_local_items(_Host, ["mod_logdb_settings"], _Server, _Lang) ->
    {result, []};
get_local_items(_Host, Item, _Server, _Lang) ->
    ?MYDEBUG("asked for items in ~p", [Item]),
    {error, ?ERR_ITEM_NOT_FOUND}.

-define(INFO_RESULT(Allow, Feats),
    case Allow of
        deny ->
            {error, ?ERR_FORBIDDEN};
        allow ->
            {result, Feats}
    end).

get_local_features(Acc, From, #jid{lserver = LServer} = _To, Node, _Lang) ->
    case gen_mod:is_loaded(LServer, mod_adhoc) of
        false ->
            Acc;
        _ ->
            LNode = string:tokens(Node, "/"),
            AllowUser = acl:match_rule(LServer, mod_logdb, From),
            AllowAdmin = acl:match_rule(LServer, mod_logdb_admin, From),
            case LNode of
                 ["mod_logdb"] when AllowUser == allow; AllowAdmin == allow ->
                    ?INFO_RESULT(allow, [?NS_COMMANDS]);
                 ["mod_logdb"] ->
                    ?INFO_RESULT(deny, [?NS_COMMANDS]);
                 ["mod_logdb_users"] ->
                    ?INFO_RESULT(AllowAdmin, []);
                 ["mod_logdb_users", [$@ | _]] ->
                    ?INFO_RESULT(AllowAdmin, []);
                 ["mod_logdb_users", _User] ->
                    ?INFO_RESULT(AllowAdmin, [?NS_COMMANDS]);
                 ["mod_logdb_settings"] ->
                    ?INFO_RESULT(AllowAdmin, [?NS_COMMANDS]);
                 [] ->
                    Acc;
                 _ ->
                    %?MYDEBUG("asked for ~p features: ~p", [LNode, Allow]),
                    Acc
            end
    end.

-define(INFO_IDENTITY(Category, Type, Name, Lang),
        [{xmlelement, "identity",
          [{"category", Category},
           {"type", Type},
           {"name", translate:translate(Lang, Name)}], []}]).

-define(INFO_COMMAND(Name, Lang),
        ?INFO_IDENTITY("automation", "command-node", Name, Lang)).

get_local_identity(Acc, _From, _To, Node, Lang) ->
    LNode = string:tokens(Node, "/"),
    case LNode of
         ["mod_logdb"] ->
            ?INFO_COMMAND("Messages logging engine", Lang);
         ["mod_logdb_users"] ->
            ?INFO_COMMAND("Messages logging engine users", Lang);
         ["mod_logdb_users", [$@ | _]] ->
            Acc;
         ["mod_logdb_users", User] ->
            ?INFO_COMMAND(User, Lang);
         ["mod_logdb_settings"] ->
            ?INFO_COMMAND("Messages logging engine settings", Lang);
         [] ->
            Acc;
         _ ->
            Acc
    end.

%get_sm_items(Acc, From, To, Node, Lang) ->
%    ?MYDEBUG("get_sm_items Acc=~p From=~p To=~p Node=~p Lang=~p", [Acc, From, To, Node, Lang]),
%    Acc.

%get_sm_features(Acc, From, To, Node, Lang) ->
%    ?MYDEBUG("get_sm_features Acc=~p From=~p To=~p Node=~p Lang=~p", [Acc, From, To, Node, Lang]),
%    Acc.

%get_sm_identity(Acc, From, To, Node, Lang) ->
%    ?MYDEBUG("get_sm_identity Acc=~p From=~p To=~p Node=~p Lang=~p", [Acc, From, To, Node, Lang]),
%    Acc.

adhoc_local_items(Acc, From, #jid{lserver = LServer, server = Server} = To,
                  Lang) ->
    Items = case Acc of
                {result, Its} -> Its;
                empty -> []
            end,
    Nodes = recursively_get_local_items(LServer, "", Server, Lang),
    Nodes1 = lists:filter(
               fun(N) ->
                        Nd = xml:get_tag_attr_s("node", N),
                        F = get_local_features([], From, To, Nd, Lang),
                        case F of
                            {result, [?NS_COMMANDS]} ->
                                true;
                            _ ->
                                false
                        end
               end, Nodes),
    {result, Items ++ Nodes1}.

recursively_get_local_items(_LServer, "mod_logdb_users", _Server, _Lang) ->
    [];
recursively_get_local_items(LServer, Node, Server, Lang) ->
    LNode = string:tokens(Node, "/"),
    Items = case get_local_items(LServer, LNode, Server, Lang) of
                {result, Res} ->
                    Res;
                {error, _Error} ->
                    []
            end,
    Nodes = lists:flatten(
      lists:map(
        fun(N) ->
                S = xml:get_tag_attr_s("jid", N),
                Nd = xml:get_tag_attr_s("node", N),
                if (S /= Server) or (Nd == "") ->
                    [];
                true ->
                    [N, recursively_get_local_items(
                          LServer, Nd, Server, Lang)]
                end
        end, Items)),
    Nodes.

-define(COMMANDS_RESULT(Allow, From, To, Request),
    case Allow of
        deny ->
            {error, ?ERR_FORBIDDEN};
        allow ->
            adhoc_local_commands(From, To, Request)
    end).

adhoc_local_commands(Acc, From, #jid{lserver = LServer} = To,
                     #adhoc_request{node = Node} = Request) ->
    LNode = string:tokens(Node, "/"),
    AllowUser = acl:match_rule(LServer, mod_logdb, From),
    AllowAdmin = acl:match_rule(LServer, mod_logdb_admin, From),
    case LNode of
         ["mod_logdb"] when AllowUser == allow; AllowAdmin == allow ->
             ?COMMANDS_RESULT(allow, From, To, Request);
         ["mod_logdb_users", _User] when AllowAdmin == allow ->
             ?COMMANDS_RESULT(allow, From, To, Request);
         ["mod_logdb_settings"] when AllowAdmin == allow ->
             ?COMMANDS_RESULT(allow, From, To, Request);
         _ ->
             Acc
    end.

adhoc_local_commands(From, #jid{lserver = LServer} = _To,
                     #adhoc_request{lang = Lang,
                                    node = Node,
                                    sessionid = SessionID,
                                    action = Action,
                                    xdata = XData} = Request) ->
    LNode = string:tokens(Node, "/"),
    %% If the "action" attribute is not present, it is
    %% understood as "execute".  If there was no <actions/>
    %% element in the first response (which there isn't in our
    %% case), "execute" and "complete" are equivalent.
    ActionIsExecute = lists:member(Action,
                                   ["", "execute", "complete"]),
    if  Action == "cancel" ->
            %% User cancels request
            adhoc:produce_response(
              Request,
              #adhoc_response{status = canceled});
        XData == false, ActionIsExecute ->
            %% User requests form
            case get_form(LServer, LNode, From, Lang) of
                {result, Form} ->
                    adhoc:produce_response(
                      Request,
                      #adhoc_response{status = executing,
                                      elements = Form});
                {error, Error} ->
                    {error, Error}
            end;
        XData /= false, ActionIsExecute ->
            %% User returns form.
            case jlib:parse_xdata_submit(XData) of
                invalid ->
                    {error, ?ERR_BAD_REQUEST};
                Fields ->
                    case set_form(From, LServer, LNode, Lang, Fields) of
                        {result, _Res} ->
                            adhoc:produce_response(
                              #adhoc_response{lang = Lang,
                                              node = Node,
                                              sessionid = SessionID,
                                              status = completed});
                        {error, Error} ->
                            {error, Error}
                    end
            end;
        true ->
            {error, ?ERR_BAD_REQUEST}
    end.

-define(LISTLINE(Label, Value),
                 {xmlelement, "option", [{"label", Label}],
                  [{xmlelement, "value", [], [{xmlcdata, Value}]}]}).
-define(DEFVAL(Value), {xmlelement, "value", [], [{xmlcdata, Value}]}).

get_user_form(LUser, LServer, Lang) ->
    %From = jlib:jid_to_string(jlib:jid_remove_resource(Jid)),
    #user_settings{dolog_default=DLD,
                   dolog_list=DLL,
                   donotlog_list=DNLL} = get_user_settings(LUser, LServer),
    {result, [{xmlelement, "x", [{"xmlns", ?NS_XDATA}],
               [{xmlelement, "title", [],
                 [{xmlcdata,
                   translate:translate(
                     Lang, "Messages logging engine settings")}]},
                {xmlelement, "instructions", [],
                 [{xmlcdata,
                   translate:translate(
                     Lang, "Set logging preferences")++ ": " ++ LUser ++ "@" ++ LServer}]},
                % default to log
                {xmlelement, "field", [{"type", "list-single"},
                                       {"label",
                                        translate:translate(Lang, "Default")},
                                       {"var", "dolog_default"}],
                 [?DEFVAL(atom_to_list(DLD)),
                  ?LISTLINE(translate:translate(Lang, "Log Messages"), "true"),
                  ?LISTLINE(translate:translate(Lang, "Do Not Log Messages"), "false")
                 ]},
                % do log list
                {xmlelement, "field", [{"type", "text-multi"},
                                       {"label",
                                        translate:translate(
                                          Lang, "Log Messages")},
                                       {"var", "dolog_list"}],
                 [{xmlelement, "value", [], [{xmlcdata, list_to_string(DLL)}]}]},
                % do not log list
                {xmlelement, "field", [{"type", "text-multi"},
                                       {"label",
                                        translate:translate(
                                          Lang, "Do Not Log Messages")},
                                       {"var", "donotlog_list"}],
                 [{xmlelement, "value", [], [{xmlcdata, list_to_string(DNLL)}]}]}
             ]}]}.

get_settings_form(Host, Lang) ->
    #state{dbmod=DBMod,
           dbs=DBs,
           dolog_default=DLD,
           ignore_jids=IgnoreJids,
           groupchat=GroupChat,
           purge_older_days=PurgeDaysT,
           drop_messages_on_user_removal=MRemoval,
           poll_users_settings=PollTime} = mod_logdb:get_module_settings(Host),

    Backends = lists:map(fun({Backend, _Opts}) ->
                             ?LISTLINE(atom_to_list(Backend), atom_to_list(Backend))
                         end, DBs),
    DB = lists:sublist(atom_to_list(DBMod), length(atom_to_list(?MODULE)) + 2, length(atom_to_list(DBMod))),
    DBsL = lists:append([?DEFVAL(DB)], Backends),

    PurgeDays =
       case PurgeDaysT of
            never -> "never";
            Num when is_integer(Num) -> integer_to_list(Num);
            _ -> "unknown"
       end,
    {result, [{xmlelement, "x", [{"xmlns", ?NS_XDATA}],
               [{xmlelement, "title", [],
                 [{xmlcdata,
                   translate:translate(
                     Lang, "Messages logging engine settings") ++ " (run-time)"}]},
                {xmlelement, "instructions", [],
                 [{xmlcdata,
                   translate:translate(
                     Lang, "Set run-time settings")}]},
                % backends
                {xmlelement, "field", [{"type", "list-single"},
                                  {"label",
                                      translate:translate(Lang, "Backend")},
                                  {"var", "backend"}],
                 DBsL},
                % dbs
                {xmlelement, "field", [{"type", "text-multi"},
                                       {"label",
                                        translate:translate(
                                          Lang, "dbs")},
                                       {"var", "dbs"}],
                 [{xmlelement, "value", [], [{xmlcdata, lists:flatten(io_lib:format("~p.",[DBs]))}]}]},
                % default to log
                {xmlelement, "field", [{"type", "list-single"},
                                       {"label",
                                        translate:translate(Lang, "Default")},
                                       {"var", "dolog_default"}],
                 [?DEFVAL(atom_to_list(DLD)),
                  ?LISTLINE(translate:translate(Lang, "Log Messages"), "true"),
                  ?LISTLINE(translate:translate(Lang, "Do Not Log Messages"), "false")
                 ]},
                % drop_messages_on_user_removal
                {xmlelement, "field", [{"type", "list-single"},
                                       {"label",
                                        translate:translate(Lang, "Drop messages on user removal")},
                                       {"var", "drop_messages_on_user_removal"}],
                 [?DEFVAL(atom_to_list(MRemoval)),
                  ?LISTLINE(translate:translate(Lang, "Drop"), "true"),
                  ?LISTLINE(translate:translate(Lang, "Do not drop"), "false")
                 ]},
                % groupchat
                {xmlelement, "field", [{"type", "list-single"},
                                       {"label",
                                        translate:translate(Lang, "Groupchat messages logging")},
                                       {"var", "groupchat"}],
                 [?DEFVAL(atom_to_list(GroupChat)),
                  ?LISTLINE("all", "all"),
                  ?LISTLINE("none", "none"),
                  ?LISTLINE("send", "send"),
                  ?LISTLINE("half", "half")
                 ]},
                % ignore_jids
                {xmlelement, "field", [{"type", "text-multi"},
                                       {"label",
                                        translate:translate(
                                          Lang, "Jids/Domains to ignore")},
                                       {"var", "ignore_list"}],
                 [{xmlelement, "value", [], [{xmlcdata, list_to_string(IgnoreJids)}]}]},
                % purge older days
                {xmlelement, "field", [{"type", "text-single"},
                                       {"label",
                                        translate:translate(
                                          Lang, "Purge messages older than (days)")},
                                       {"var", "purge_older_days"}],
                 [{xmlelement, "value", [], [{xmlcdata, PurgeDays}]}]},
                % poll users settings
                {xmlelement, "field", [{"type", "text-single"},
                                       {"label",
                                        translate:translate(
                                          Lang, "Poll users settings (seconds)")},
                                       {"var", "poll_users_settings"}],
                 [{xmlelement, "value", [], [{xmlcdata, integer_to_list(PollTime)}]}]}
             ]}]}.

get_form(_Host, ["mod_logdb"], #jid{luser = LUser, lserver = LServer} = _Jid, Lang) ->
    get_user_form(LUser, LServer, Lang);
get_form(_Host, ["mod_logdb_users", User], _JidFrom, Lang) ->
    #jid{luser=LUser, lserver=LServer} = jlib:string_to_jid(User),
    get_user_form(LUser, LServer, Lang);
get_form(Host, ["mod_logdb_settings"], _JidFrom, Lang) ->
    get_settings_form(Host, Lang);
get_form(_Host, Command, _, _Lang) ->
    ?MYDEBUG("asked for form ~p", [Command]),
    {error, ?ERR_SERVICE_UNAVAILABLE}.

check_log_list([Head | Tail]) ->
    case lists:member($@, Head) of
         true -> ok;
         false -> throw(error)
    end,
    % this check for Head to be valid jid
    case jlib:string_to_jid(Head) of
         error ->
            throw(error);
         _ ->
            check_log_list(Tail)
    end;
check_log_list([]) ->
    ok.

check_ignore_list([Head | Tail]) ->
    case lists:member($@, Head) of
         true -> ok;
         false -> throw(error)
    end,
    % this check for Head to be valid jid
    case jlib:string_to_jid(Head) of
         error ->
            % this check for Head to be valid domain "@domain.org"
            case lists:nth(1, Head) of
                 $@ ->
                    % TODO: this allows spaces and special characters in Head. May be change to nodeprep?
                    case jlib:nameprep(lists:delete($@, Head)) of
                         error -> throw(error);
                         _ -> check_log_list(Tail)
                    end;
                 _ -> throw(error)
            end;
         _ ->
            check_ignore_list(Tail)
    end;
check_ignore_list([]) ->
    ok.

parse_users_settings(XData) ->
    DLD = case lists:keysearch("dolog_default", 1, XData) of
               {value, {_, [String]}} when String == "true"; String == "false" -> 
                 list_to_bool(String);
               _ ->
                 throw(bad_request)
          end,
    DLL = case lists:keysearch("dolog_list", 1, XData) of
               false ->
                 throw(bad_request);
               {value, {_, [[]]}} ->
                 [];
               {value, {_, List1}} ->
                 case catch check_log_list(List1) of
                      error ->
                         throw(bad_request);
                      ok ->
                         List1
                 end
          end,
    DNLL = case lists:keysearch("donotlog_list", 1, XData) of
               false ->
                 throw(bad_request);
               {value, {_, [[]]}} ->
                 [];
               {value, {_, List2}} ->
                 case catch check_log_list(List2) of
                      error ->
                         throw(bad_request);
                      ok ->
                         List2
                 end
          end,
    #user_settings{dolog_default=DLD,
                   dolog_list=DLL,
                   donotlog_list=DNLL}.

parse_module_settings(XData) ->
    DLD = case lists:keysearch("dolog_default", 1, XData) of
               {value, {_, [Str1]}} when Str1 == "true"; Str1 == "false" ->
                 list_to_bool(Str1);
               _ ->
                 throw(bad_request)
          end,
    MRemoval = case lists:keysearch("drop_messages_on_user_removal", 1, XData) of
               {value, {_, [Str5]}} when Str5 == "true"; Str5 == "false" ->
                 list_to_bool(Str5);
               _ ->
                 throw(bad_request)
          end,
    GroupChat = case lists:keysearch("groupchat", 1, XData) of
                     {value, {_, [Str2]}} when Str2 == "none";
                                               Str2 == "all";
                                               Str2 == "send";
                                               Str2 == "half" ->
                       list_to_atom(Str2);
                     _ ->
                       throw(bad_request)
                end,
    Ignore = case lists:keysearch("ignore_list", 1, XData) of
                  {value, {_, List}} ->
                    case catch check_ignore_list(List) of
                         ok ->
                            List;
                         error ->
                            throw(bad_request)
                    end;
                  _ ->
                    throw(bad_request)
             end,
    Purge = case lists:keysearch("purge_older_days", 1, XData) of
                 {value, {_, ["never"]}} ->
                   never;
                 {value, {_, [Str3]}} ->
                   case catch list_to_integer(Str3) of
                        {'EXIT', {badarg, _}} -> throw(bad_request);
                        Int1 -> Int1
                   end;
                 _ ->
                   throw(bad_request)
            end,
    Poll = case lists:keysearch("poll_users_settings", 1, XData) of
                {value, {_, [Str4]}} ->
                  case catch list_to_integer(Str4) of
                       {'EXIT', {badarg, _}} -> throw(bad_request);
                       Int2 -> Int2
                  end;
                _ ->
                  throw(bad_request)
           end,
    #state{dolog_default=DLD,
           groupchat=GroupChat,
           ignore_jids=Ignore,
           purge_older_days=Purge,
           drop_messages_on_user_removal=MRemoval,
           poll_users_settings=Poll}.

set_form(From, _Host, ["mod_logdb"], _Lang, XData) ->
    #jid{luser=LUser, lserver=LServer} = From,
    case catch parse_users_settings(XData) of
         bad_request ->
            {error, ?ERR_BAD_REQUEST};
         UserSettings ->
            case mod_logdb:set_user_settings(LUser, LServer, UserSettings) of
                 ok ->
                    {result, []};
                 error ->
                    {error, ?ERR_INTERNAL_SERVER_ERROR}
            end
    end;
set_form(_From, _Host, ["mod_logdb_users", User], _Lang, XData) ->
    #jid{luser=LUser, lserver=LServer} = jlib:string_to_jid(User),
    case catch parse_users_settings(XData) of
         bad_request -> {error, ?ERR_BAD_REQUEST};
         UserSettings ->
            case mod_logdb:set_user_settings(LUser, LServer, UserSettings) of
                 ok ->
                    {result, []};
                 error ->
                    {error, ?ERR_INTERNAL_SERVER_ERROR}
            end
    end;
set_form(_From, Host, ["mod_logdb_settings"], _Lang, XData) ->
    case catch parse_module_settings(XData) of
         bad_request -> {error, ?ERR_BAD_REQUEST};
         Settings ->
            case mod_logdb:set_module_settings(Host, Settings) of
                 ok ->
                    {result, []};
                 error ->
                    {error, ?ERR_INTERNAL_SERVER_ERROR}
            end
    end;
set_form(From, _Host, Node, _Lang, XData) ->
    User = jlib:jid_to_string(jlib:jid_remove_resource(From)),
    ?MYDEBUG("set form for ~p at ~p XData=~p", [User, Node, XData]),
    {error, ?ERR_SERVICE_UNAVAILABLE}.

%adhoc_sm_items(Acc, From, To, Request) ->
%    ?MYDEBUG("adhoc_sm_items Acc=~p From=~p To=~p Request=~p", [Acc, From, To, Request]),
%    Acc.

%adhoc_sm_commands(Acc, From, To, Request) ->
%    ?MYDEBUG("adhoc_sm_commands Acc=~p From=~p To=~p Request=~p", [Acc, From, To, Request]),
%    Acc.

get_all_vh_users(Host, Server, Lang) ->
    case catch ejabberd_auth:get_vh_registered_users(Host) of
        {'EXIT', _Reason} ->
            [];
        Users ->
            SUsers = lists:sort([{S, U} || {U, S} <- Users]),
            case length(SUsers) of
                N when N =< 100 ->
                    lists:map(fun({S, U}) ->
                                      ?NODE(U ++ "@" ++ S, "mod_logdb_users/" ++ U ++ "@" ++ S)
                              end, SUsers);
                N ->
                    NParts = trunc(math:sqrt(N * 0.618)) + 1,
                    M = trunc(N / NParts) + 1,
                    lists:map(fun(K) ->
                                      L = K + M - 1,
                                      Node =
                                          "@" ++ integer_to_list(K) ++
                                          "-" ++ integer_to_list(L),
                                      {FS, FU} = lists:nth(K, SUsers),
                                      {LS, LU} =
                                          if L < N -> lists:nth(L, SUsers);
                                             true -> lists:last(SUsers)
                                          end,
                                      Name =
                                          FU ++ "@" ++ FS ++
                                          " -- " ++
                                          LU ++ "@" ++ LS,
                                      ?NODE(Name, "mod_logdb_users/" ++ Node)
                              end, lists:seq(1, N, M))
            end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% webadmin hooks
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
webadmin_menu(Acc, _Host, Lang) ->
    [{"messages", ?T("Users Messages")} | Acc].

webadmin_user(Acc, User, Server, Lang) ->
    Sett = get_user_settings(User, Server),
    Log =
      case Sett#user_settings.dolog_default of
           false ->
              ?INPUTT("submit", "dolog", "Log Messages");
           true ->
              ?INPUTT("submit", "donotlog", "Do Not Log Messages");
           _ -> []
      end,
    Acc ++ [?XE("h3", [?ACT("messages/", "Messages"), ?C(" "), Log])].

webadmin_page(_, Host,
              #request{path = ["messages"],
                       q = Query,
                       lang = Lang}) when is_list(Host) ->
    Res = vhost_messages_stats(Host, Query, Lang),
    {stop, Res};
webadmin_page(_, Host,
              #request{path = ["messages", Date],
                       q = Query,
                       lang = Lang}) when is_list(Host) ->
    Res = vhost_messages_stats_at(Host, Query, Lang, Date),
    {stop, Res};
webadmin_page(_, Host,
              #request{path = ["user", U, "messages"],
                       q = Query,
                       lang = Lang}) ->
    Res = user_messages_stats(U, Host, Query, Lang),
    {stop, Res};
webadmin_page(_, Host,
              #request{path = ["user", U, "messages", Date],
                       q = Query,
                       lang = Lang}) ->
    Res = mod_logdb:user_messages_stats_at(U, Host, Query, Lang, Date),
    {stop, Res};
webadmin_page(Acc, _, _) -> Acc.

user_parse_query(_, "dolog", User, Server, _Query) ->
    Sett = get_user_settings(User, Server),
    % TODO: check returned value
    set_user_settings(User, Server, Sett#user_settings{dolog_default=true}),
    {stop, ok};
user_parse_query(_, "donotlog", User, Server, _Query) ->
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
              [?XC("h1", ?T("Error occupied while fetching list"))];
         {error, GReason} ->
              ?ERROR_MSG("Failed to get_vhost_stats: ~p", [GReason]),
              [?XC("h1", ?T("Error occupied while fetching list"))];
         {ok, []} ->
              [?XC("h1", ?T("No logged messages for ") ++ Server)];
         {ok, Dates} ->
              Fun = fun({Date, Count}) ->
                         ID = jlib:encode_base64(binary_to_list(term_to_binary(Server++Date))),
                         ?XE("tr",
                          [?XE("td", [?INPUT("checkbox", "selected", ID)]),
                           ?XE("td", [?AC(Date, Date)]),
                           ?XC("td", integer_to_list(Count))
                          ])
                    end,
              [?XC("h1", ?T("Logged messages for ") ++ Server)] ++
               case Res of
                    ok -> [?CT("Submitted"), ?P];
                    error -> [?CT("Bad format"), ?P];
                    nothing -> []
               end ++
               [?XAE("form", [{"action", ""}, {"method", "post"}],
                [?XE("table",
                 [?XE("thead",
                  [?XE("tr",
                   [?X("td"),
                    ?XCT("td", "Date"),
                    ?XCT("td", "Count")
                   ])]),
                  ?XE("tbody",
                      lists:map(Fun, Dates)
                     )]),
                  ?BR,
                  ?INPUTT("submit", "delete", "Delete Selected")
                ])]
   end.

vhost_messages_stats_at(Server, Query, Lang, Date) ->
   {Time, Value} = timer:tc(mod_logdb, get_vhost_stats_at, [Server, Date]),
   ?INFO_MSG("get_vhost_stats_at(~p,~p) elapsed ~p sec", [Server, Date, Time/1000000]),
   %case get_vhost_stats_at(Server, Date) of
   case Value of
        {'EXIT', CReason} ->
             ?ERROR_MSG("Failed to get_vhost_stats_at: ~p", [CReason]),
             [?XC("h1", ?T("Error occupied while fetching list"))];
        {error, GReason} ->
             ?ERROR_MSG("Failed to get_vhost_stats_at: ~p", [GReason]),
             [?XC("h1", ?T("Error occupied while fetching list"))];
        {ok, []} ->
             [?XC("h1", ?T("No logged messages for ") ++ Server ++ ?T(" at ") ++ Date)];
        {ok, Users} ->
             Res = case catch vhost_messages_at_parse_query(Server, Date, Users, Query) of
                        {'EXIT', Reason} ->
                            ?ERROR_MSG("~p", [Reason]),
                            error;
                        VResult -> VResult
                   end,
             Fun = fun({User, Count}) ->
                         ID = jlib:encode_base64(binary_to_list(term_to_binary(User++Server))),
                         ?XE("tr",
                          [?XE("td", [?INPUT("checkbox", "selected", ID)]),
                           ?XE("td", [?AC("../user/"++User++"/messages/"++Date, User)]),
                           ?XC("td", integer_to_list(Count))
                          ])
                   end,
             [?XC("h1", ?T("Logged messages for ") ++ Server ++ ?T(" at ") ++ Date)] ++
              case Res of
                    ok -> [?CT("Submitted"), ?P];
                    error -> [?CT("Bad format"), ?P];
                    nothing -> []
              end ++
              [?XAE("form", [{"action", ""}, {"method", "post"}],
                [?XE("table",
                 [?XE("thead",
                  [?XE("tr",
                   [?X("td"),
                    ?XCT("td", "User"),
                    ?XCT("td", "Count")
                   ])]),
                  ?XE("tbody",
                      lists:map(Fun, Users)
                     )]),
                  ?BR,
                  ?INPUTT("submit", "delete", "Delete Selected")
                ])]
   end.

user_messages_stats(User, Server, Query, Lang) ->
    Jid = jlib:jid_to_string({User, Server, ""}),

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
            [?XC("h1", ?T("Error occupied while fetching days"))];
        {error, GReason} ->
            ?ERROR_MSG("Failed to get_user_stats: ~p", [GReason]),
            [?XC("h1", ?T("Error occupied while fetching days"))];
        {ok, []} ->
            [?XC("h1", ?T("No logged messages for ") ++ Jid)];
        {ok, Dates} ->
            Fun = fun({Date, Count}) ->
                      ID = jlib:encode_base64(binary_to_list(term_to_binary(User++Date))),
                      ?XE("tr",
                       [?XE("td", [?INPUT("checkbox", "selected", ID)]),
                        ?XE("td", [?AC(Date, Date)]),
                        ?XC("td", integer_to_list(Count))
                       ])
                       %[?AC(Date, Date ++ " (" ++ integer_to_list(Count) ++ ")"), ?BR]
                  end,
            [?XC("h1", ?T("Logged messages for ") ++ Jid)] ++
             case Res of
                   ok -> [?CT("Submitted"), ?P];
                   error -> [?CT("Bad format"), ?P];
                   nothing -> []
             end ++
             [?XAE("form", [{"action", ""}, {"method", "post"}],
              [?XE("table",
               [?XE("thead",
                [?XE("tr",
                 [?X("td"),
                  ?XCT("td", "Date"),
                  ?XCT("td", "Count")
                 ])]),
                ?XE("tbody",
                    lists:map(Fun, Dates)
                   )]),
                ?BR,
                ?INPUTT("submit", "delete", "Delete Selected")
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
   Jid = jlib:jid_to_string({User, Server, ""}),

   {Time, Value} = timer:tc(mod_logdb, get_user_messages_at, [User, Server, Date]),
   ?INFO_MSG("get_user_messages_at(~p,~p,~p) elapsed ~p sec", [User, Server, Date, Time/1000000]),
   case Value of
        {'EXIT', CReason} ->
           ?ERROR_MSG("Failed to get_user_messages_at: ~p", [CReason]),
           [?XC("h1", ?T("Error occupied while fetching messages"))];
        {error, GReason} ->
           ?ERROR_MSG("Failed to get_user_messages_at: ~p", [GReason]),
           [?XC("h1", ?T("Error occupied while fetching messages"))];
        {ok, []} ->
           [?XC("h1", ?T("No logged messages for ") ++ Jid ++ ?T(" at ") ++ Date)];
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
                              {jlib:jid_to_string(Item#roster.jid), Item#roster.name}
                           end, UR),

           UniqUsers = lists:foldl(fun(#msg{peer_name=PName, peer_server=PServer}, List) ->
                                 ToAdd = PName++"@"++PServer,
                                 case lists:member(ToAdd, List) of
                                      true -> List;
                                      false -> lists:append([ToAdd], List)
                                 end
                               end, [], User_messages),

           % Users to filter (sublist of UniqUsers)
           CheckedUsers = case lists:keysearch("filter", 1, Query) of
                           {value, _} ->
                              lists:filter(fun(UFUser) ->
                                                ID = jlib:encode_base64(binary_to_list(term_to_binary(UFUser))),
                                                lists:member({"selected", ID}, Query)
                                           end, UniqUsers);
                           false -> []
                         end,

           % UniqUsers in html (noone selected -> everyone selected)
           Users = lists:map(fun(UHUser) ->
                                ID = jlib:encode_base64(binary_to_list(term_to_binary(UHUser))),
                                Input = case lists:member(UHUser, CheckedUsers) of
                                         true -> [?INPUTC("checkbox", "selected", ID)];
                                         false when CheckedUsers == [] -> [?INPUTC("checkbox", "selected", ID)];
                                         false -> [?INPUT("checkbox", "selected", ID)]
                                        end,
                                Nick =
                                   case search_user_nick(UHUser, UserRoster) of
                                        nothing -> "";
                                        N -> " ("++ N ++")"
                                   end,
                                ?XE("tr",
                                 [?XE("td", Input),
                                  ?XC("td", UHUser++Nick)])
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
                      TextRaw = case Subject of
                                     "" -> Body;
                                     _ -> [?T("Subject"),": ",Subject,"<br>", Body]
                                end,
                      ID = jlib:encode_base64(binary_to_list(term_to_binary(Timestamp))),
                      % replace \n with <br>
                      Text = lists:map(fun(10) -> "<br>";
                                           (A) -> A
                                        end, TextRaw),
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
                      ?XE("tr",
                       [?XE("td", [?INPUT("checkbox", "selected", ID)]),
                        ?XC("td", convert_timestamp(Timestamp)),
                        ?XC("td", atom_to_list(Direction)++": "++UserNick),
                        ?XC("td", Text)])
                 end,
           % Filtered user messages in html
           Msgs = lists:map(Msgs_Fun, lists:sort(User_messages_filtered)),

           [?XC("h1", ?T("Logged messages for ") ++ Jid ++ ?T(" at ") ++ Date)] ++
            case Res of
                 ok -> [?CT("Submitted"), ?P];
                 error -> [?CT("Bad format"), ?P];
                 nothing -> []
            end ++
            [?XAE("form", [{"action", ""}, {"method", "post"}],
             [?XE("table",
                  [?XE("thead",
                       [?X("td"),
                        ?XCT("td", "User")
                       ]
                      ),
                   ?XE("tbody",
                        Users
                      )]),
              ?INPUTT("submit", "filter", "Filter Selected")
             ] ++
             [?XE("table",
                  [?XE("thead",
                       [?XE("tr",
                        [?X("td"),
                         ?XCT("td", "Date, Time"),
                         ?XCT("td", "Direction: Jid"),
                         ?XCT("td", "Body")
                        ])]),
                   ?XE("tbody",
                        Msgs
                      )]),
              ?INPUTT("submit", "delete", "Delete Selected"),
              ?BR
             ]
            )]
    end.
