%%%----------------------------------------------------------------------
%%% File    : mod_logdb_mysql5.erl
%%% Author  : Oleg Palij (mailto:o.palij@gmail.com)
%%% Purpose : MySQL 5 backend for mod_logdb
%%% Url     : https://paleg.github.io/mod_logdb/
%%%----------------------------------------------------------------------

-module(mod_logdb_mysql5).
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

% gen_server call timeout
-define(CALL_TIMEOUT, 30000).
-define(MYSQL_TIMEOUT, 60000).
-define(INDEX_SIZE, integer_to_list(170)).
-define(PROCNAME, mod_logdb_mysql5).

-import(mod_logdb, [list_to_bool/1, bool_to_list/1,
                    list_to_string/1, string_to_list/1,
                    convert_timestamp_brief/1]).

-record(state, {dbref, vhost, server, port, db, user, password}).

% replace "." with "_"
escape_vhost(VHost) -> lists:map(fun(46) -> 95;
                                    (A) -> A
                                 end, binary_to_list(VHost)).
prefix() ->
   "`logdb_".

suffix(VHost) ->
   "_" ++ escape_vhost(VHost) ++ "`".

messages_table(VHost, Date) ->
   prefix() ++ "messages_" ++ Date ++ suffix(VHost).

% TODO: this needs to be redone to unify view name in stored procedure and in delete_messages_at/2
view_table(VHost, Date) ->
   Table = messages_table(VHost, Date),
   TablewoQ = lists:sublist(Table, 2, length(Table) - 2),
   lists:append(["`v_", TablewoQ, "`"]).

stats_table(VHost) ->
   prefix() ++ "stats" ++ suffix(VHost).

temp_table(VHost) ->
   prefix() ++ "temp" ++ suffix(VHost).

settings_table(VHost) ->
   prefix() ++ "settings" ++ suffix(VHost).

users_table(VHost) ->
   prefix() ++ "users" ++ suffix(VHost).
servers_table(VHost) ->
   prefix() ++ "servers" ++ suffix(VHost).
resources_table(VHost) ->
   prefix() ++ "resources" ++ suffix(VHost).

logmessage_name(VHost) ->
   prefix() ++ "logmessage" ++ suffix(VHost).

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
init([VHost, Opts]) ->
   crypto:start(),

   Server = gen_mod:get_opt(server, Opts, fun(A) -> A end, <<"localhost">>),
   Port = gen_mod:get_opt(port, Opts, fun(A) -> A end, 3306),
   DB = gen_mod:get_opt(db, Opts, fun(A) -> A end, <<"logdb">>),
   User = gen_mod:get_opt(user, Opts, fun(A) -> A end, <<"root">>),
   Password = gen_mod:get_opt(password, Opts, fun(A) -> A end, <<"">>),

   St = #state{vhost=VHost,
               server=Server, port=Port, db=DB,
               user=User, password=Password},

   case open_mysql_connection(St) of
       {ok, DBRef} ->
           State = St#state{dbref=DBRef},
           ok = create_internals(State),
           ok = create_stats_table(State),
           ok = create_settings_table(State),
           ok = create_users_table(State),
           ok = create_servers_table(State),
           ok = create_resources_table(State),
           erlang:monitor(process, DBRef),
           {ok, State};
       {error, Reason} ->
           ?ERROR_MSG("MySQL connection failed: ~p~n", [Reason]),
           {stop, db_connection_failed}
   end.

open_mysql_connection(#state{server=Server, port=Port, db=DB,
                             user=DBUser, password=Password} = _State) ->
   LogFun = fun(debug, _Format, _Argument) ->
                 %?MYDEBUG(Format, Argument);
                 ok;
               (error, Format, Argument) ->
                 ?ERROR_MSG(Format, Argument);
               (Level, Format, Argument) ->
                 ?MYDEBUG("MySQL (~p)~n", [Level]),
                 ?MYDEBUG(Format, Argument)
            end,
   ?INFO_MSG("Opening mysql connection ~s@~s:~p/~s", [DBUser, Server, Port, DB]),
   p1_mysql_conn:start(binary_to_list(Server), Port,
                       binary_to_list(DBUser), binary_to_list(Password),
                       binary_to_list(DB), LogFun).

close_mysql_connection(DBRef) ->
   ?MYDEBUG("Closing ~p mysql connection", [DBRef]),
   catch p1_mysql_conn:stop(DBRef).

handle_call({rebuild_stats_at, Date}, _From, #state{dbref=DBRef, vhost=VHost}=State) ->
    Reply = rebuild_stats_at_int(DBRef, VHost, Date),
    {reply, Reply, State};
handle_call({delete_messages_by_user_at, [], _Date}, _From, State) ->
    {reply, error, State};
handle_call({delete_messages_by_user_at, Msgs, Date}, _From, #state{dbref=DBRef, vhost=VHost}=State) ->
    Temp = lists:flatmap(fun(#msg{timestamp=Timestamp} = _Msg) ->
                             ["\"",Timestamp,"\"",","]
                         end, Msgs),

    Temp1 = lists:append([lists:sublist(Temp, length(Temp)-1), ");"]),

    Query = ["DELETE FROM ",messages_table(VHost, Date)," ",
                             "WHERE timestamp IN (", Temp1],

    Reply =
      case sql_query_internal(DBRef, Query) of
           {updated, Aff} ->
              ?MYDEBUG("Aff=~p", [Aff]),
              rebuild_stats_at_int(DBRef, VHost, Date);
           {error, _} ->
              error
      end,
    {reply, Reply, State};
handle_call({delete_all_messages_by_user_at, User, Date}, _From, #state{dbref=DBRef, vhost=VHost}=State) ->
    ok = delete_all_messages_by_user_at_int(DBRef, User, VHost, Date),
    ok = delete_stats_by_user_at_int(DBRef, User, VHost, Date),
    {reply, ok, State};
handle_call({delete_messages_at, Date}, _From, #state{dbref=DBRef, vhost=VHost}=State) ->
    Fun = fun() ->
              {updated, _} = sql_query_internal(DBRef, ["DROP TABLE ",messages_table(VHost, Date),";"]),
              TQuery = ["DELETE FROM ",stats_table(VHost)," "
                           "WHERE at=\"",Date,"\";"],
              {updated, _} = sql_query_internal(DBRef, TQuery),
              VQuery = ["DROP VIEW IF EXISTS ",view_table(VHost,Date),";"],
              {updated, _} = sql_query_internal(DBRef, VQuery),
              ok
          end,
    Reply =
      case catch apply(Fun, []) of
           ok ->
              ok;
           {'EXIT', _} ->
              error
      end,
    {reply, Reply, State};
handle_call({get_vhost_stats}, _From, #state{dbref=DBRef, vhost=VHost}=State) ->
    SName = stats_table(VHost),
    Query = ["SELECT at, sum(count) ",
                "FROM ",SName," ",
                "GROUP BY at ",
                "ORDER BY DATE(at) DESC;"
            ],
    Reply =
      case sql_query_internal(DBRef, Query) of
           {data, Result} ->
              {ok, [ {Date, list_to_integer(Count)} || [Date, Count] <- Result ]};
           {error, Reason} ->
              % TODO: Duplicate error message ?
              {error, Reason}
      end,
    {reply, Reply, State};
handle_call({get_vhost_stats_at, Date}, _From, #state{dbref=DBRef, vhost=VHost}=State) ->
    SName = stats_table(VHost),
    Query = ["SELECT username, sum(count) as allcount ",
                "FROM ",SName," ",
                "JOIN ",users_table(VHost)," ON owner_id=user_id "
                "WHERE at=\"",Date,"\" ",
                "GROUP BY username ",
                "ORDER BY allcount DESC;"
            ],
    Reply =
      case sql_query_internal(DBRef, Query) of
           {data, Result} ->
              {ok, [ {User, list_to_integer(Count)} || [User, Count] <- Result ]};
           {error, Reason} ->
              {error, Reason}
      end,
    {reply, Reply, State};
handle_call({get_user_stats, User}, _From, #state{dbref=DBRef, vhost=VHost}=State) ->
    {reply, get_user_stats_int(DBRef, User, VHost), State};
handle_call({get_user_messages_at, User, Date}, _From, #state{dbref=DBRef, vhost=VHost}=State) ->
    Query = ["SELECT peer_name,",
                    "peer_server,",
                    "peer_resource,",
                    "direction,"
                    "type,"
                    "subject,"
                    "body,"
                    "timestamp "
               "FROM ",view_table(VHost, Date)," "
               "WHERE owner_name=\"",User,"\";"],
    Reply =
      case sql_query_internal(DBRef, Query) of
           {data, Result} ->
              Fun = fun([Peer_name, Peer_server, Peer_resource,
                         Direction,
                         Type,
                         Subject, Body,
                         Timestamp]) ->
                          #msg{peer_name=Peer_name, peer_server=Peer_server, peer_resource=Peer_resource,
                               direction=list_to_atom(Direction),
                               type=Type,
                               subject=Subject, body=Body,
                               timestamp=Timestamp}
                    end,
              {ok, lists:map(Fun, Result)};
           {error, Reason} ->
              {error, Reason}
      end,
    {reply, Reply, State};
handle_call({get_dates}, _From, #state{dbref=DBRef, vhost=VHost}=State) ->
    SName = stats_table(VHost),
    Query = ["SELECT at ",
                "FROM ",SName," ",
                "GROUP BY at ",
                "ORDER BY DATE(at) DESC;"
            ],
    Reply =
       case sql_query_internal(DBRef, Query) of
            {data, Result} ->
               [ Date || [Date] <- Result ];
            {error, Reason} ->
               {error, Reason}
       end,
    {reply, Reply, State};
handle_call({get_users_settings}, _From, #state{dbref=DBRef, vhost=VHost}=State) ->
    Query = ["SELECT username,dolog_default,dolog_list,donotlog_list ",
                "FROM ",settings_table(VHost)," ",
             "JOIN ",users_table(VHost)," ON user_id=owner_id;"],
    Reply =
      case sql_query_internal(DBRef, Query) of
           {data, Result} ->
              {ok, lists:map(fun([Owner, DoLogDef, DoLogL, DoNotLogL]) ->
                                 #user_settings{owner_name=Owner,
                                                dolog_default=list_to_bool(DoLogDef),
                                                dolog_list=string_to_list(DoLogL),
                                                donotlog_list=string_to_list(DoNotLogL)
                                               }
                             end, Result)};
           {error, _} ->
              error
      end,
    {reply, Reply, State};
handle_call({get_user_settings, User}, _From, #state{dbref=DBRef, vhost=VHost}=State) ->
    Query = ["SELECT dolog_default,dolog_list,donotlog_list FROM ",settings_table(VHost)," ",
                 "WHERE owner_id=(SELECT user_id FROM ",users_table(VHost)," WHERE username=\"",User,"\");"],
    Reply =
      case sql_query_internal(DBRef, Query) of
           {data, []} ->
              {ok, []};
           {data, [[Owner, DoLogDef, DoLogL, DoNotLogL]]} ->
              {ok, #user_settings{owner_name=Owner,
                                  dolog_default=list_to_bool(DoLogDef),
                                  dolog_list=string_to_list(DoLogL),
                                  donotlog_list=string_to_list(DoNotLogL)}};
           {error, _} ->
              error
      end,
    {reply, Reply, State};
handle_call({set_user_settings, User, #user_settings{dolog_default=DoLogDef,
                                                     dolog_list=DoLogL,
                                                     donotlog_list=DoNotLogL}},
            _From, #state{dbref=DBRef, vhost=VHost} = State) ->
    User_id = get_user_id(DBRef, VHost, User),
    Query = ["UPDATE ",settings_table(VHost)," ",
                "SET dolog_default=",bool_to_list(DoLogDef),", ",
                    "dolog_list='",list_to_string(DoLogL),"', ",
                    "donotlog_list='",list_to_string(DoNotLogL),"' ",
                "WHERE owner_id=",User_id,";"],

    Reply =
      case sql_query_internal(DBRef, Query) of
           {updated, 0} ->
              IQuery = ["INSERT INTO ",settings_table(VHost)," ",
                            "(owner_id, dolog_default, dolog_list, donotlog_list) ",
                            "VALUES ",
                            "(",User_id,",",bool_to_list(DoLogDef),",'",list_to_string(DoLogL),"','",list_to_string(DoNotLogL),"');"],
              case sql_query_internal_silent(DBRef, IQuery) of
                   {updated, _} ->
                       ?MYDEBUG("New settings for ~s@~s", [User, VHost]),
                       ok;
                   {error, Reason} ->
                       case ejabberd_regexp:run(iolist_to_binary(Reason), <<"#23000">>) of
                            % Already exists
                            match ->
                                ok;
                             _ ->
                                ?ERROR_MSG("Failed setup user ~p@~p: ~p", [User, VHost, Reason]),
                                error
                       end
              end;
           {updated, 1} ->
              ?MYDEBUG("Updated settings for ~s@~s", [User, VHost]),
              ok;
           {error, _} ->
              error
      end,
    {reply, Reply, State};
handle_call({stop}, _From, #state{vhost=VHost}=State) ->
   ?MYDEBUG("Stoping mysql5 backend for ~p", [VHost]),
   {stop, normal, ok, State};
handle_call(Msg, _From, State) ->
    ?INFO_MSG("Got call Msg: ~p, State: ~p", [Msg, State]),
    {noreply, State}.

handle_cast({log_message, Msg}, #state{dbref=DBRef, vhost=VHost}=State) ->
    Fun = fun() ->
            Date = convert_timestamp_brief(Msg#msg.timestamp),
            TableName = messages_table(VHost, Date),

            Query = [ "CALL ",logmessage_name(VHost)," "
                         "('", TableName, "',",
                         "'", Date, "',",
                         "'", binary_to_list(Msg#msg.owner_name), "',",
                         "'", binary_to_list(Msg#msg.peer_name), "',",
                         "'", binary_to_list(Msg#msg.peer_server), "',",
                         "'", binary_to_list( ejabberd_sql:escape(Msg#msg.peer_resource) ), "',",
                         "'", atom_to_list(Msg#msg.direction), "',",
                         "'", binary_to_list(Msg#msg.type), "',",
                         "'", binary_to_list( ejabberd_sql:escape(Msg#msg.subject) ), "',",
                         "'", binary_to_list( ejabberd_sql:escape(Msg#msg.body) ), "',",
                         "'", Msg#msg.timestamp, "');"],

            case sql_query_internal(DBRef, Query) of
                 {updated, _} ->
                    ?MYDEBUG("Logged ok for ~s, peer: ~s", [ [Msg#msg.owner_name, <<"@">>, VHost],
                                                             [Msg#msg.peer_name, <<"@">>, Msg#msg.peer_server] ]),
                    ok;
                 {error, _Reason} ->
                    error
            end
          end,
    spawn(Fun),
    {noreply, State};
handle_cast({rebuild_stats}, State) ->
    rebuild_all_stats_int(State),
    {noreply, State};
handle_cast({drop_user, User}, #state{vhost=VHost} = State) ->
    Fun = fun() ->
            {ok, DBRef} = open_mysql_connection(State),
            {ok, Dates} = get_user_stats_int(DBRef, User, VHost),
            MDResult = lists:map(fun({Date, _}) ->
                           delete_all_messages_by_user_at_int(DBRef, User, VHost, Date)
                       end, Dates),
            StDResult = delete_all_stats_by_user_int(DBRef, User, VHost),
            SDResult = delete_user_settings_int(DBRef, User, VHost),
            case lists:all(fun(Result) when Result == ok ->
                                true;
                              (Result) when Result == error ->
                               false
                           end, lists:append([MDResult, [StDResult], [SDResult]])) of
                 true ->
                   ?INFO_MSG("Removed ~s@~s", [User, VHost]);
                 false ->
                   ?ERROR_MSG("Failed to remove ~s@~s", [User, VHost])
            end,
            close_mysql_connection(DBRef)
          end,
    spawn(Fun),
    {noreply, State};
handle_cast(Msg, State) ->
    ?INFO_MSG("Got cast Msg:~p, State:~p", [Msg, State]),
    {noreply, State}.

handle_info({'DOWN', _MonitorRef, process, _Pid, _Info}, State) ->
    {stop, connection_dropped, State};
handle_info(Info, State) ->
    ?INFO_MSG("Got Info:~p, State:~p", [Info, State]),
    {noreply, State}.

terminate(_Reason, #state{dbref=DBRef}=_State) ->
    close_mysql_connection(DBRef),
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
   gen_server:cast(Proc, {log_message, Msg}).
rebuild_stats(VHost) ->
   Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
   gen_server:cast(Proc, {rebuild_stats}).
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
get_users_settings(VHost) ->
   Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
   gen_server:call(Proc, {get_users_settings}, ?CALL_TIMEOUT).
get_user_settings(User, VHost) ->
   Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
   gen_server:call(Proc, {get_user_settings, User}, ?CALL_TIMEOUT).
set_user_settings(User, VHost, Set) ->
   Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
   gen_server:call(Proc, {set_user_settings, User, Set}, ?CALL_TIMEOUT).
drop_user(User, VHost) ->
   Proc = gen_mod:get_module_proc(VHost, ?PROCNAME),
   gen_server:cast(Proc, {drop_user, User}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% internals
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_dates_int(DBRef, VHost) ->
    case sql_query_internal(DBRef, ["SHOW TABLES"]) of
         {data, Tables} ->
            Reg = "^" ++ lists:sublist(prefix(),2,length(prefix())) ++ ".*" ++ escape_vhost(VHost),
            lists:foldl(fun([Table], Dates) ->
                           case re:run(Table, Reg) of
                                {match, _} ->
                                   case re:run(Table, "[0-9]+-[0-9]+-[0-9]+") of
                                        {match, [{S, E}]} ->
                                            lists:append(Dates, [lists:sublist(Table, S+1, E)]);
                                        nomatch ->
                                            Dates
                                   end;
                                _ ->
                                   Dates
                           end
                        end, [], Tables);
         {error, _} ->
            []
    end.

rebuild_all_stats_int(#state{vhost=VHost}=State) ->
    Fun = fun() ->
             {ok, DBRef} = open_mysql_connection(State),
             ok = delete_nonexistent_stats(DBRef, VHost),
             case lists:filter(fun(Date) ->
                                 case catch rebuild_stats_at_int(DBRef, VHost, Date) of
                                      ok -> false;
                                      error -> true;
                                      {'EXIT', _} -> true
                                 end
                             end, get_dates_int(DBRef, VHost)) of
                  [] -> ok;
                  FTables ->
                     ?ERROR_MSG("Failed to rebuild stats for ~p dates", [FTables]),
                     error
             end,
             close_mysql_connection(DBRef)
          end,
    spawn(Fun).

rebuild_stats_at_int(DBRef, VHost, Date) ->
    TempTable = temp_table(VHost),
    Fun = fun() ->
           Table = messages_table(VHost, Date),
           STable = stats_table(VHost),

           DQuery = [ "DELETE FROM ",STable," ",
                          "WHERE at='",Date,"';"],

           ok = create_temp_table(DBRef, TempTable),
           {updated, _} = sql_query_internal(DBRef, ["LOCK TABLE ",Table," WRITE, ",TempTable," WRITE;"]),
           SQuery = ["INSERT INTO ",TempTable," ",
                      "(owner_id,peer_name_id,peer_server_id,at,count) ",
                         "SELECT owner_id,peer_name_id,peer_server_id,\"",Date,"\",count(*) ",
                            "FROM ",Table," WHERE ext is NULL GROUP BY owner_id,peer_name_id,peer_server_id;"],
           case sql_query_internal(DBRef, SQuery) of
                  {updated, 0} ->
                      Count = sql_query_internal(DBRef, ["SELECT count(*) FROM ",Table,";"]),
                      case Count of
                        {data, [["0"]]} ->
                           {updated, _} = sql_query_internal(DBRef, ["DROP TABLE ",Table,";"]),
                           sql_query_internal(DBRef, ["UNLOCK TABLES;"]),
                           {updated, _} = sql_query_internal(DBRef, ["DROP VIEW IF EXISTS ",view_table(VHost,Date),";"]),
                           {updated, _} = sql_query_internal(DBRef, ["LOCK TABLE ",STable," WRITE, ",TempTable," WRITE;"]),
                           {updated, _} = sql_query_internal(DBRef, DQuery),
                           ok;
                        _ ->
                           ?ERROR_MSG("Failed to calculate stats for ~s table! Count was ~p.", [Date, Count]),
                           error
                      end;
                  {updated, _} ->
                      {updated, _} = sql_query_internal(DBRef, ["LOCK TABLE ",STable," WRITE, ",TempTable," WRITE;"]),
                      {updated, _} = sql_query_internal(DBRef, DQuery),
                      SQuery1 = ["INSERT INTO ",STable," ",
                                  "(owner_id,peer_name_id,peer_server_id,at,count) ",
                                     "SELECT owner_id,peer_name_id,peer_server_id,at,count ",
                                        "FROM ",TempTable,";"],
                      case sql_query_internal(DBRef, SQuery1) of
                           {updated, _} -> ok;
                           {error, _} -> error
                      end;
                  {error, _} -> error
           end
       end,

    case catch apply(Fun, []) of
         ok ->
           ?INFO_MSG("Rebuilded stats for ~p at ~p", [VHost, Date]),
           ok;
         error ->
           error;
         {'EXIT', Reason} ->
           ?ERROR_MSG("Failed to rebuild stats for ~s table: ~p.", [Date, Reason]),
           error
    end,
    sql_query_internal(DBRef, ["UNLOCK TABLES;"]),
    sql_query_internal(DBRef, ["DROP TABLE ",TempTable,";"]),
    ok.

delete_nonexistent_stats(DBRef, VHost) ->
    Dates = get_dates_int(DBRef, VHost),
    STable = stats_table(VHost),

    Temp = lists:flatmap(fun(Date) ->
                             ["\"",Date,"\"",","]
                         end, Dates),
    case Temp of
         [] ->
           ok;
         _ ->
           % replace last "," with ");"
           Temp1 = lists:append([lists:sublist(Temp, length(Temp)-1), ");"]),
           Query = ["DELETE FROM ",STable," ",
                       "WHERE at NOT IN (", Temp1],
           case sql_query_internal(DBRef, Query) of
                {updated, _} ->
                    ok;
                {error, _} ->
                    error
           end
    end.

get_user_stats_int(DBRef, User, VHost) ->
    SName = stats_table(VHost),
    UName = users_table(VHost),
    Query = ["SELECT stats.at, sum(stats.count) ",
                "FROM ",UName," AS users ",
                   "JOIN ",SName," AS stats ON owner_id=user_id "
                "WHERE users.username=\"",User,"\" ",
                "GROUP BY stats.at "
                "ORDER BY DATE(stats.at) DESC;"
            ],
    case sql_query_internal(DBRef, Query) of
         {data, Result} ->
            {ok, [ {Date, list_to_integer(Count)} || [Date, Count] <- Result ]};
         {error, Result} ->
            {error, Result}
    end.

delete_all_messages_by_user_at_int(DBRef, User, VHost, Date) ->
    DQuery = ["DELETE FROM ",messages_table(VHost, Date)," ",
                 "WHERE owner_id=(SELECT user_id FROM ",users_table(VHost)," WHERE username=\"",User,"\");"],
    case sql_query_internal(DBRef, DQuery) of
         {updated, _} ->
            ?INFO_MSG("Dropped messages for ~s@~s at ~s", [User, VHost, Date]),
            ok;
         {error, _} ->
            error
    end.

delete_all_stats_by_user_int(DBRef, User, VHost) ->
    SQuery = ["DELETE FROM ",stats_table(VHost)," ",
                "WHERE owner_id=(SELECT user_id FROM ",users_table(VHost)," WHERE username=\"",User,"\");"],
    case sql_query_internal(DBRef, SQuery) of
         {updated, _} ->
             ?INFO_MSG("Dropped all stats for ~s@~s", [User, VHost]),
             ok;
         {error, _} -> error
    end.

delete_stats_by_user_at_int(DBRef, User, VHost, Date) ->
    SQuery = ["DELETE FROM ",stats_table(VHost)," ",
                "WHERE owner_id=(SELECT user_id FROM ",users_table(VHost)," WHERE username=\"",User,"\") ",
                  "AND at=\"",Date,"\";"],
    case sql_query_internal(DBRef, SQuery) of
         {updated, _} ->
             ?INFO_MSG("Dropped stats for ~s@~s at ~s", [User, VHost, Date]),
             ok;
         {error, _} -> error
    end.

delete_user_settings_int(DBRef, User, VHost) ->
    Query = ["DELETE FROM ",settings_table(VHost)," ",
                 "WHERE owner_id=(SELECT user_id FROM ",users_table(VHost)," WHERE username=\"",User,"\");"],
    case sql_query_internal(DBRef, Query) of
         {updated, _} ->
            ?INFO_MSG("Dropped ~s@~s settings", [User, VHost]),
            ok;
         {error, Reason} ->
            ?ERROR_MSG("Failed to drop ~s@~s settings: ~p", [User, VHost, Reason]),
            error
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% tables internals
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
create_temp_table(DBRef, Name) ->
    Query = ["CREATE TABLE ",Name," (",
                "owner_id MEDIUMINT UNSIGNED, ",
                "peer_name_id MEDIUMINT UNSIGNED, ",
                "peer_server_id MEDIUMINT UNSIGNED, ",
                "at VARCHAR(11), ",
                "count INT(11) ",
             ") ENGINE=MyISAM CHARACTER SET utf8;"
            ],
    case sql_query_internal(DBRef, Query) of
         {updated, _} -> ok;
         {error, _Reason} -> error
    end.

create_stats_table(#state{dbref=DBRef, vhost=VHost}=State) ->
    SName = stats_table(VHost),
    Query = ["CREATE TABLE ",SName," (",
                "owner_id MEDIUMINT UNSIGNED, ",
                "peer_name_id MEDIUMINT UNSIGNED, ",
                "peer_server_id MEDIUMINT UNSIGNED, ",
                "at VARCHAR(11), ",
                "count INT(11), ",
                "ext INTEGER DEFAULT NULL, "
                "INDEX ext_i (ext), "
                "INDEX(owner_id,peer_name_id,peer_server_id), ",
                "INDEX(at) ",
             ") ENGINE=MyISAM CHARACTER SET utf8;"
            ],
    case sql_query_internal_silent(DBRef, Query) of
         {updated, _} ->
            ?MYDEBUG("Created stats table for ~p", [VHost]),
            rebuild_all_stats_int(State),
            ok;
         {error, Reason} ->
            case ejabberd_regexp:run(iolist_to_binary(Reason), <<"#42S01">>) of
                 match ->
                   ?MYDEBUG("Stats table for ~p already exists", [VHost]),
                   CheckQuery = ["SHOW COLUMNS FROM ",SName," LIKE 'peer_%_id';"],
                   case sql_query_internal(DBRef, CheckQuery) of
                        {data, Elems} when length(Elems) == 2 ->
                          ?MYDEBUG("Stats table structure is ok", []),
                          ok;
                        _ ->
                          ?INFO_MSG("It seems like stats table structure is invalid. I will drop it and recreate", []),
                          case sql_query_internal(DBRef, ["DROP TABLE ",SName,";"]) of
                               {updated, _} ->
                                  ?INFO_MSG("Successfully dropped ~p", [SName]);
                               _ ->
                                  ?ERROR_MSG("Failed to drop ~p. You should drop it and restart module", [SName])
                          end,
                          error
                   end;
                 _ ->
                   ?ERROR_MSG("Failed to create stats table for ~p: ~p", [VHost, Reason]),
                   error
            end
    end.

create_settings_table(#state{dbref=DBRef, vhost=VHost}) ->
    SName = settings_table(VHost),
    Query = ["CREATE TABLE IF NOT EXISTS ",SName," (",
                "owner_id MEDIUMINT UNSIGNED PRIMARY KEY, ",
                "dolog_default TINYINT(1) NOT NULL DEFAULT 1, ",
                "dolog_list TEXT, ",
                "donotlog_list TEXT ",
             ") ENGINE=InnoDB CHARACTER SET utf8;"
            ],
    case sql_query_internal(DBRef, Query) of
         {updated, _} ->
            ?MYDEBUG("Created settings table for ~p", [VHost]),
            ok;
         {error, _} ->
            error
    end.

create_users_table(#state{dbref=DBRef, vhost=VHost}) ->
    SName = users_table(VHost),
    Query = ["CREATE TABLE IF NOT EXISTS ",SName," (",
                "username TEXT NOT NULL, ",
                "user_id MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE, ",
                "UNIQUE INDEX(username(",?INDEX_SIZE,")) ",
             ") ENGINE=InnoDB CHARACTER SET utf8;"
            ],
    case sql_query_internal(DBRef, Query) of
         {updated, _} ->
            ?MYDEBUG("Created users table for ~p", [VHost]),
            ok;
         {error, _} ->
            error
    end.

create_servers_table(#state{dbref=DBRef, vhost=VHost}) ->
    SName = servers_table(VHost),
    Query = ["CREATE TABLE IF NOT EXISTS ",SName," (",
                "server TEXT NOT NULL, ",
                "server_id MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE, ",
                "UNIQUE INDEX(server(",?INDEX_SIZE,")) ",
             ") ENGINE=InnoDB CHARACTER SET utf8;"
            ],
    case sql_query_internal(DBRef, Query) of
         {updated, _} ->
            ?MYDEBUG("Created servers table for ~p", [VHost]),
            ok;
         {error, _} ->
            error
    end.

create_resources_table(#state{dbref=DBRef, vhost=VHost}) ->
    RName = resources_table(VHost),
    Query = ["CREATE TABLE IF NOT EXISTS ",RName," (",
                "resource TEXT NOT NULL, ",
                "resource_id MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE, ",
                "UNIQUE INDEX(resource(",?INDEX_SIZE,")) ",
             ") ENGINE=InnoDB CHARACTER SET utf8;"
            ],
    case sql_query_internal(DBRef, Query) of
         {updated, _} ->
            ?MYDEBUG("Created resources table for ~p", [VHost]),
            ok;
         {error, _} ->
            error
    end.

create_internals(#state{dbref=DBRef, vhost=VHost}) ->
    sql_query_internal(DBRef, ["DROP PROCEDURE IF EXISTS ",logmessage_name(VHost),";"]),
    case sql_query_internal(DBRef, [get_logmessage(VHost)]) of
         {updated, _} ->
            ?MYDEBUG("Created logmessage for ~p", [VHost]),
            ok;
         {error, _} ->
            error
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% SQL internals
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
sql_query_internal(DBRef, Query) ->
    case sql_query_internal_silent(DBRef, Query) of
         {error, Reason} ->
            ?ERROR_MSG("~p while ~p", [Reason, lists:append(Query)]),
            {error, Reason};
         Rez -> Rez
    end.

sql_query_internal_silent(DBRef, Query) ->
    ?MYDEBUG("DOING: \"~s\"", [lists:append(Query)]),
    get_result(p1_mysql_conn:fetch(DBRef, Query, self(), ?MYSQL_TIMEOUT)).

get_result({updated, MySQLRes}) ->
    {updated, p1_mysql:get_result_affected_rows(MySQLRes)};
get_result({data, MySQLRes}) ->
    {data, p1_mysql:get_result_rows(MySQLRes)};
get_result({error, "query timed out"}) ->
    {error, "query timed out"};
get_result({error, MySQLRes}) ->
    Reason = p1_mysql:get_result_reason(MySQLRes),
    {error, Reason}.

get_user_id(DBRef, VHost, User) ->
  SQuery = ["SELECT user_id FROM ",users_table(VHost)," ",
               "WHERE username=\"",User,"\";"],
  case sql_query_internal(DBRef, SQuery) of
       {data, []} ->
          IQuery = ["INSERT INTO ",users_table(VHost)," ",
                       "SET username=\"",User,"\";"],
          case sql_query_internal_silent(DBRef, IQuery) of
               {updated, _} ->
                   {data, [[DBIdNew]]} = sql_query_internal(DBRef, SQuery),
                   DBIdNew;
               {error, Reason} ->
                   % this can be in clustered environment
                   match = ejabberd_regexp:run(iolist_to_binary(Reason), <<"#23000">>),
                   ?ERROR_MSG("Duplicate key name for ~p", [User]),
                   {data, [[ClID]]} = sql_query_internal(DBRef, SQuery),
                   ClID
          end;
       {data, [[DBId]]} ->
          DBId
  end.

get_logmessage(VHost) ->
    UName = users_table(VHost),
    SName = servers_table(VHost),
    RName = resources_table(VHost),
    StName = stats_table(VHost),
    io_lib:format("
CREATE PROCEDURE ~s(tablename TEXT, atdate TEXT, owner TEXT, peer_name TEXT, peer_server TEXT, peer_resource TEXT, mdirection VARCHAR(4), mtype VARCHAR(10), msubject TEXT, mbody TEXT, mtimestamp DOUBLE)
BEGIN
   DECLARE ownerID MEDIUMINT UNSIGNED;
   DECLARE peer_nameID MEDIUMINT UNSIGNED;
   DECLARE peer_serverID MEDIUMINT UNSIGNED;
   DECLARE peer_resourceID MEDIUMINT UNSIGNED;
   DECLARE Vmtype VARCHAR(10);
   DECLARE Vmtimestamp DOUBLE;
   DECLARE Vmdirection VARCHAR(4);
   DECLARE Vmbody TEXT;
   DECLARE Vmsubject TEXT;
   DECLARE iq TEXT;
   DECLARE cq TEXT;
   DECLARE viewname TEXT;
   DECLARE notable INT;
   DECLARE CONTINUE HANDLER FOR SQLSTATE '42S02' SET @notable = 1;

   SET @notable = 0;
   SET @ownerID = NULL;
   SET @peer_nameID = NULL;
   SET @peer_serverID = NULL;
   SET @peer_resourceID = NULL;

   SET @Vmtype = mtype;
   SET @Vmtimestamp = mtimestamp;
   SET @Vmdirection = mdirection;
   SET @Vmbody = mbody;
   SET @Vmsubject = msubject;

   SELECT user_id INTO @ownerID FROM ~s WHERE username=owner;
   IF @ownerID IS NULL THEN
      INSERT INTO ~s SET username=owner;
      SET @ownerID = LAST_INSERT_ID();
   END IF;

   SELECT user_id INTO @peer_nameID FROM ~s WHERE username=peer_name;
   IF @peer_nameID IS NULL THEN
      INSERT INTO ~s SET username=peer_name;
      SET @peer_nameID = LAST_INSERT_ID();
   END IF;

   SELECT server_id INTO @peer_serverID FROM ~s WHERE server=peer_server;
   IF @peer_serverID IS NULL THEN
      INSERT INTO ~s SET server=peer_server;
      SET @peer_serverID = LAST_INSERT_ID();
   END IF;

   SELECT resource_id INTO @peer_resourceID FROM ~s WHERE resource=peer_resource;
   IF @peer_resourceID IS NULL THEN
      INSERT INTO ~s SET resource=peer_resource;
      SET @peer_resourceID = LAST_INSERT_ID();
   END IF;

   SET @iq = CONCAT(\"INSERT INTO \",tablename,\" (owner_id, peer_name_id, peer_server_id, peer_resource_id, direction, type, subject, body, timestamp) VALUES (@ownerID,@peer_nameID,@peer_serverID,@peer_resourceID,@Vmdirection,@Vmtype,@Vmsubject,@Vmbody,@Vmtimestamp);\");
   PREPARE insertmsg FROM @iq;

   IF @notable = 1 THEN
      SET @cq = CONCAT(\"CREATE TABLE \",tablename,\" (
                          owner_id MEDIUMINT UNSIGNED NOT NULL,
                          peer_name_id MEDIUMINT UNSIGNED NOT NULL,
                          peer_server_id MEDIUMINT UNSIGNED NOT NULL,
                          peer_resource_id MEDIUMINT(8) UNSIGNED NOT NULL,
                          direction ENUM('to', 'from') NOT NULL,
                          type ENUM('chat','error','groupchat','headline','normal') NOT NULL,
                          subject TEXT,
                          body TEXT,
                          timestamp DOUBLE NOT NULL,
                          ext INTEGER DEFAULT NULL,
                          INDEX search_i (owner_id, peer_name_id, peer_server_id, peer_resource_id),
                          INDEX ext_i (ext),
                          FULLTEXT (body)
                       ) ENGINE=MyISAM
                         PACK_KEYS=1
                         CHARACTER SET utf8;\");
      PREPARE createtable FROM @cq;
      EXECUTE createtable;
      DEALLOCATE PREPARE createtable;

      SET @viewname = CONCAT(\"`v_\", TRIM(BOTH '`' FROM tablename), \"`\");
      SET @cq = CONCAT(\"CREATE OR REPLACE VIEW \",@viewname,\" AS
                         SELECT owner.username AS owner_name,
                                peer.username AS peer_name,
                                servers.server AS peer_server,
                                resources.resource AS peer_resource,
                                messages.direction,
                                messages.type,
                                messages.subject,
                                messages.body,
                                messages.timestamp
                         FROM
                                ~s owner,
                                ~s peer,
                                ~s servers,
                                ~s resources,
                              \", tablename,\" messages
                         WHERE
                                owner.user_id=messages.owner_id and
                                peer.user_id=messages.peer_name_id and
                                servers.server_id=messages.peer_server_id and
                                resources.resource_id=messages.peer_resource_id
                         ORDER BY messages.timestamp;\");
      PREPARE createview FROM @cq;
      EXECUTE createview;
      DEALLOCATE PREPARE createview;

      SET @notable = 0;
      PREPARE insertmsg FROM @iq;
      EXECUTE insertmsg;
   ELSEIF @notable = 0 THEN
      EXECUTE insertmsg;
   END IF;

   DEALLOCATE PREPARE insertmsg;

   IF @notable = 0 THEN
      UPDATE ~s SET count=count+1 WHERE owner_id=@ownerID AND peer_name_id=@peer_nameID AND peer_server_id=@peer_serverID AND at=atdate;
      IF ROW_COUNT() = 0 THEN
         INSERT INTO ~s (owner_id, peer_name_id, peer_server_id, at, count) VALUES (@ownerID, @peer_nameID, @peer_serverID, atdate, 1);
      END IF;
   END IF;
END;", [logmessage_name(VHost),UName,UName,UName,UName,SName,SName,RName,RName,UName,UName,SName,RName,StName,StName]).
