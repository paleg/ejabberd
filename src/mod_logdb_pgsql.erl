% {ok, DBRef} = pgsql:connect([{host, "127.0.0.1"}, {database, "logdb"}, {user, "logdb"}, {password, "logdb"}, {port, 5432}, {as_binary, true}]).
% Schema = "test".
% pgsql:squery(DBRef, "CREATE TABLE test.\"logdb_stats_test\" (owner_id INTEGER, peer_name_id INTEGER, peer_server_id INTEGER, at VARCHAR(20), count integer);" ).
%%%----------------------------------------------------------------------
%%% File    : mod_logdb_pgsql.erl
%%% Author  : Oleg Palij (mailto:o.palij@gmail.com)
%%% Purpose : Posgresql backend for mod_logdb
%%% Url     : https://paleg.github.io/mod_logdb/
%%%----------------------------------------------------------------------

-module(mod_logdb_pgsql).
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

-export([view_table/3]).

% gen_server call timeout
-define(CALL_TIMEOUT, 30000).
-define(PGSQL_TIMEOUT, 60000).
-define(PROCNAME, mod_logdb_pgsql).

-import(mod_logdb, [list_to_bool/1, bool_to_list/1,
                    list_to_string/1, string_to_list/1,
                    convert_timestamp_brief/1]).

-record(state, {dbref, vhost, server, port, db, user, password, schema}).

% replace "." with "_"
escape_vhost(VHost) -> lists:map(fun(46) -> 95;
                                    (A) -> A
                                 end, binary_to_list(VHost)).

prefix(Schema) ->
   Schema ++ ".\"" ++ "logdb_".

suffix(VHost) ->
   "_" ++ escape_vhost(VHost) ++ "\"".

messages_table(VHost, Schema, Date) ->
   prefix(Schema) ++ "messages_" ++ Date ++ suffix(VHost).

view_table(VHost, Schema, Date) ->
   Table = messages_table(VHost, Schema, Date),
   TablewoS = lists:sublist(Table, length(Schema) + 3, length(Table) - length(Schema) - 3),
   lists:append([Schema, ".\"v_", TablewoS, "\""]).

stats_table(VHost, Schema) ->
   prefix(Schema) ++ "stats" ++ suffix(VHost).

temp_table(VHost, Schema) ->
   prefix(Schema) ++ "temp" ++ suffix(VHost).

settings_table(VHost, Schema) ->
   prefix(Schema) ++ "settings" ++ suffix(VHost).

users_table(VHost, Schema) ->
   prefix(Schema) ++ "users" ++ suffix(VHost).
servers_table(VHost, Schema) ->
   prefix(Schema) ++ "servers" ++ suffix(VHost).
resources_table(VHost, Schema) ->
   prefix(Schema) ++ "resources" ++ suffix(VHost).

logmessage_name(VHost, Schema) ->
   prefix(Schema) ++ "logmessage" ++ suffix(VHost).

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
   Server = gen_mod:get_opt(server, Opts, fun(A) -> A end, <<"localhost">>),
   DB = gen_mod:get_opt(db, Opts, fun(A) -> A end, <<"ejabberd_logdb">>),
   User = gen_mod:get_opt(user, Opts, fun(A) -> A end, <<"root">>),
   Port = gen_mod:get_opt(port, Opts, fun(A) -> A end, 5432),
   Password = gen_mod:get_opt(password, Opts, fun(A) -> A end, <<"">>),
   Schema = binary_to_list(gen_mod:get_opt(schema, Opts, fun(A) -> A end, <<"public">>)),

   ?MYDEBUG("Starting pgsql backend for ~s", [VHost]),

   St = #state{vhost=VHost,
               server=Server, port=Port, db=DB,
               user=User, password=Password,
               schema=Schema},

   case open_pgsql_connection(St) of
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
       % this does not work
       {error, Reason} ->
           ?ERROR_MSG("PgSQL connection failed: ~p~n", [Reason]),
           {stop, db_connection_failed};
       % and this too, becouse pgsql_conn do exit() which can not be catched
       {'EXIT', Rez} ->
           ?ERROR_MSG("Rez: ~p~n", [Rez]),
           {stop, db_connection_failed}
   end.

open_pgsql_connection(#state{server=Server, port=Port, db=DB, schema=Schema,
                             user=User, password=Password} = _State) ->
   ?INFO_MSG("Opening pgsql connection ~s@~s:~p/~s", [User, Server, Port, DB]),
   {ok, DBRef} = pgsql:connect(Server, DB, User, Password, Port),
   {updated, _} = sql_query_internal(DBRef, ["SET SEARCH_PATH TO ",Schema,";"]),
   {ok, DBRef}.

close_pgsql_connection(DBRef) ->
   ?MYDEBUG("Closing ~p pgsql connection", [DBRef]),
   pgsql:terminate(DBRef).

handle_call({log_message, Msg}, _From, #state{dbref=DBRef, vhost=VHost, schema=Schema}=State) ->
    Date = convert_timestamp_brief(Msg#msg.timestamp),
    TableName = messages_table(VHost, Schema, Date),
    ViewName = view_table(VHost, Schema, Date),

    Query = [ "SELECT ", logmessage_name(VHost, Schema)," "
                 "('", TableName, "',",
                  "'", ViewName, "',",
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

    case sql_query_internal_silent(DBRef, Query) of
    % TODO: change this
         {data, [{"0"}]} ->
             ?MYDEBUG("Logged ok for ~s, peer: ~s", [ [Msg#msg.owner_name, <<"@">>, VHost],
                                                      [Msg#msg.peer_name, <<"@">>, Msg#msg.peer_server] ]),
             ok;
         {error, _Reason} ->
             error
    end,
    {reply, ok, State};
handle_call({rebuild_stats_at, Date}, _From, #state{dbref=DBRef, vhost=VHost, schema=Schema}=State) ->
    Reply = rebuild_stats_at_int(DBRef, VHost, Schema, Date),
    {reply, Reply, State};
handle_call({delete_messages_by_user_at, [], _Date}, _From, State) ->
    {reply, error, State};
handle_call({delete_messages_by_user_at, Msgs, Date}, _From, #state{dbref=DBRef, vhost=VHost, schema=Schema}=State) ->
    Temp = lists:flatmap(fun(#msg{timestamp=Timestamp} = _Msg) ->
                             ["'",Timestamp,"'",","]
                         end, Msgs),

    Temp1 = lists:append([lists:sublist(Temp, length(Temp)-1), ");"]),

    Query = ["DELETE FROM ",messages_table(VHost, Schema, Date)," ",
                             "WHERE timestamp IN (", Temp1],

    Reply =
      case sql_query_internal(DBRef, Query) of
           {updated, _} ->
              rebuild_stats_at_int(DBRef, VHost, Schema, Date);
           {error, _} ->
              error
      end,
    {reply, Reply, State};
handle_call({delete_all_messages_by_user_at, User, Date}, _From, #state{dbref=DBRef, vhost=VHost, schema=Schema}=State) ->
    ok = delete_all_messages_by_user_at_int(DBRef, Schema, User, VHost, Date),
    ok = delete_stats_by_user_at_int(DBRef, Schema, User, VHost, Date),
    {reply, ok, State};
handle_call({delete_messages_at, Date}, _From, #state{dbref=DBRef, vhost=VHost, schema=Schema}=State) ->
    {updated, _} = sql_query_internal(DBRef, ["DROP VIEW ",view_table(VHost, Schema, Date),";"]),
    Reply =
      case sql_query_internal(DBRef, ["DROP TABLE ",messages_table(VHost, Schema, Date)," CASCADE;"]) of
           {updated, _} ->
              Query = ["DELETE FROM ",stats_table(VHost, Schema)," "
                          "WHERE at='",Date,"';"],
              case sql_query_internal(DBRef, Query) of
                   {updated, _} ->
                      ok;
                   {error, _} ->
                      error
              end;
           {error, _} ->
              error
      end,
    {reply, Reply, State};
handle_call({get_vhost_stats}, _From, #state{dbref=DBRef, vhost=VHost, schema=Schema}=State) ->
    SName = stats_table(VHost, Schema),
    Query = ["SELECT at, sum(count) ",
                "FROM ",SName," ",
                "GROUP BY at ",
                "ORDER BY DATE(at) DESC;"
            ],
    Reply =
      case sql_query_internal(DBRef, Query) of
           {data, Recs} ->
              {ok, [ {Date, list_to_integer(Count)} || {Date, Count} <- Recs]};
           {error, Reason} ->
              % TODO: Duplicate error message ?
              {error, Reason}
      end,
    {reply, Reply, State};
handle_call({get_vhost_stats_at, Date}, _From, #state{dbref=DBRef, vhost=VHost, schema=Schema}=State) ->
    SName = stats_table(VHost, Schema),
    Query = ["SELECT username, sum(count) AS allcount ",
                "FROM ",SName," ",
                "JOIN ",users_table(VHost, Schema)," ON owner_id=user_id ",
                "WHERE at='",Date,"' ",
                "GROUP BY username ",
                "ORDER BY allcount DESC;"
            ],
    Reply =
      case sql_query_internal(DBRef, Query) of
           {data, Recs} ->
              RFun = fun({User, Count}) ->
                          {User, list_to_integer(Count)}
                     end,
              {ok, lists:reverse(lists:keysort(2, lists:map(RFun, Recs)))};
           {error, Reason} ->
              % TODO:
              {error, Reason}
      end,
    {reply, Reply, State};
handle_call({get_user_stats, User}, _From, #state{dbref=DBRef, vhost=VHost, schema=Schema}=State) ->
    {reply, get_user_stats_int(DBRef, Schema, User, VHost), State};
handle_call({get_user_messages_at, User, Date}, _From, #state{dbref=DBRef, vhost=VHost, schema=Schema}=State) ->
    Query = ["SELECT peer_name,",
                    "peer_server,",
                    "peer_resource,",
                    "direction,"
                    "type,"
                    "subject,"
                    "body,"
                    "timestamp "
               "FROM ",view_table(VHost, Schema, Date)," "
               "WHERE owner_name='",User,"';"],
    Reply =
      case sql_query_internal(DBRef, Query) of
           {data, Recs} ->
              Fun = fun({Peer_name, Peer_server, Peer_resource,
                         Direction,
                         Type,
                         Subject, Body,
                         Timestamp}) ->
                          #msg{peer_name=Peer_name, peer_server=Peer_server, peer_resource=Peer_resource,
                               direction=list_to_atom(Direction),
                               type=Type,
                               subject=Subject, body=Body,
                               timestamp=Timestamp}
                    end,
              {ok, lists:map(Fun, Recs)};
           {error, Reason} ->
              {error, Reason}
      end,
    {reply, Reply, State};
handle_call({get_dates}, _From, #state{dbref=DBRef, vhost=VHost, schema=Schema}=State) ->
    SName = stats_table(VHost, Schema),
    Query = ["SELECT at ",
                "FROM ",SName," ",
                "GROUP BY at ",
                "ORDER BY at DESC;"
            ],
    Reply =
       case sql_query_internal(DBRef, Query) of
            {data, Result} ->
               [ Date || {Date} <- Result ];
            {error, Reason} ->
               {error, Reason}
       end,
    {reply, Reply, State};
handle_call({get_users_settings}, _From, #state{dbref=DBRef, vhost=VHost, schema=Schema}=State) ->
    Query = ["SELECT username,dolog_default,dolog_list,donotlog_list ",
                "FROM ",settings_table(VHost, Schema)," ",
             "JOIN ",users_table(VHost, Schema)," ON user_id=owner_id;"],
    Reply =
      case sql_query_internal(DBRef, Query) of
           {data, Recs} ->
              {ok, [#user_settings{owner_name=Owner,
                                   dolog_default=list_to_bool(DoLogDef),
                                   dolog_list=string_to_list(DoLogL),
                                   donotlog_list=string_to_list(DoNotLogL)
                                  } || {Owner, DoLogDef, DoLogL, DoNotLogL} <- Recs]};
           {error, Reason} ->
              {error, Reason}
      end,
    {reply, Reply, State};
handle_call({get_user_settings, User}, _From, #state{dbref=DBRef, vhost=VHost, schema=Schema}=State) ->
    Query = ["SELECT dolog_default,dolog_list,donotlog_list ",
                "FROM ",settings_table(VHost, Schema)," ",
             "WHERE owner_id=(SELECT user_id FROM ",users_table(VHost, Schema)," WHERE username='",User,"');"],
    Reply =
      case sql_query_internal_silent(DBRef, Query) of
           {data, []} ->
              {ok, []};
           {data, [{DoLogDef, DoLogL, DoNotLogL}]} ->
              {ok, #user_settings{owner_name=User,
                                  dolog_default=list_to_bool(DoLogDef),
                                  dolog_list=string_to_list(DoLogL),
                                  donotlog_list=string_to_list(DoNotLogL)}};
           {error, Reason} ->
              ?ERROR_MSG("Failed to get_user_settings for ~s@~s: ~p", [User, VHost, Reason]),
              error
      end,
    {reply, Reply, State};
handle_call({set_user_settings, User, #user_settings{dolog_default=DoLogDef,
                                                     dolog_list=DoLogL,
                                                     donotlog_list=DoNotLogL}},
            _From, #state{dbref=DBRef, vhost=VHost, schema=Schema}=State) ->
    User_id = get_user_id(DBRef, VHost, Schema, User),
    Query = ["UPDATE ",settings_table(VHost, Schema)," ",
                "SET dolog_default=",bool_to_list(DoLogDef),", ",
                    "dolog_list='",list_to_string(DoLogL),"', ",
                    "donotlog_list='",list_to_string(DoNotLogL),"' ",
                "WHERE owner_id=",User_id,";"],

    Reply =
      case sql_query_internal(DBRef, Query) of
           {updated, 0} ->
              IQuery = ["INSERT INTO ",settings_table(VHost, Schema)," ",
                            "(owner_id, dolog_default, dolog_list, donotlog_list) ",
                            "VALUES ",
                            "(",User_id,", ",bool_to_list(DoLogDef),",'",list_to_string(DoLogL),"','",list_to_string(DoNotLogL),"');"],
              case sql_query_internal(DBRef, IQuery) of
                   {updated, 1} ->
                       ?MYDEBUG("New settings for ~s@~s", [User, VHost]),
                       ok;
                   {error, _} ->
                       error
              end;
           {updated, 1} ->
              ?MYDEBUG("Updated settings for ~s@~s", [User, VHost]),
              ok;
           {error, _} ->
              error
      end,
    {reply, Reply, State};
handle_call({stop}, _From, State) ->
   ?MYDEBUG("Stoping pgsql backend for ~p", [State#state.vhost]),
   {stop, normal, ok, State};
handle_call(Msg, _From, State) ->
    ?INFO_MSG("Got call Msg: ~p, State: ~p", [Msg, State]),
    {noreply, State}.


handle_cast({rebuild_stats}, State) ->
    rebuild_all_stats_int(State),
    {noreply, State};
handle_cast({drop_user, User}, #state{vhost=VHost, schema=Schema}=State) ->
    Fun = fun() ->
            {ok, DBRef} = open_pgsql_connection(State),
            {ok, Dates} = get_user_stats_int(DBRef, Schema, User, VHost),
            MDResult = lists:map(fun({Date, _}) ->
                           delete_all_messages_by_user_at_int(DBRef, Schema, User, VHost, Date)
                       end, Dates),
            StDResult = delete_all_stats_by_user_int(DBRef, Schema, User, VHost),
            SDResult = delete_user_settings_int(DBRef, Schema, User, VHost),
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
            close_pgsql_connection(DBRef)
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
    close_pgsql_connection(DBRef),
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
    Query = ["SELECT n.nspname as \"Schema\",
                c.relname as \"Name\",
                CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' END as \"Type\",
                r.rolname as \"Owner\"
              FROM pg_catalog.pg_class c
                   JOIN pg_catalog.pg_roles r ON r.oid = c.relowner
                   LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
              WHERE c.relkind IN ('r','')
                    AND n.nspname NOT IN ('pg_catalog', 'pg_toast')
                    AND c.relname ~ '^(.*",escape_vhost(VHost),".*)$'
                    AND pg_catalog.pg_table_is_visible(c.oid)
              ORDER BY 1,2;"],
    case sql_query_internal(DBRef, Query) of
         {data, Recs} ->
            lists:foldl(fun({_Schema, Table, _Type, _Owner}, Dates) ->
                             case re:run(Table,"[0-9]+-[0-9]+-[0-9]+") of
                                  {match, [{S, E}]} ->
                                      lists:append(Dates, [lists:sublist(Table, S+1, E)]);
                                  nomatch ->
                                      Dates
                             end
                        end, [], Recs);
         {error, _} ->
            []
    end.

rebuild_all_stats_int(#state{vhost=VHost, schema=Schema}=State) ->
    Fun = fun() ->
             {ok, DBRef} = open_pgsql_connection(State),
             ok = delete_nonexistent_stats(DBRef, Schema, VHost),
             case lists:filter(fun(Date) ->
                                 case catch rebuild_stats_at_int(DBRef, VHost, Schema, Date) of
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
             close_pgsql_connection(DBRef)
          end,
    spawn(Fun).

rebuild_stats_at_int(DBRef, VHost, Schema, Date) ->
    TempTable = temp_table(VHost, Schema),
    Fun =
      fun() ->
       Table = messages_table(VHost, Schema, Date),
       STable = stats_table(VHost, Schema),

       DQuery = [ "DELETE FROM ",STable," ",
                     "WHERE at='",Date,"';"],

       ok = create_temp_table(DBRef, VHost, Schema),
       {updated, _} = sql_query_internal(DBRef, ["LOCK TABLE ",Table," IN ACCESS EXCLUSIVE MODE;"]),
       {updated, _} = sql_query_internal(DBRef, ["LOCK TABLE ",TempTable," IN ACCESS EXCLUSIVE MODE;"]),
       SQuery = ["INSERT INTO ",TempTable," ",
                  "(owner_id,peer_name_id,peer_server_id,at,count) ",
                     "SELECT owner_id,peer_name_id,peer_server_id,'",Date,"'",",count(*) ",
                        "FROM ",Table," GROUP BY owner_id,peer_name_id,peer_server_id;"],
       case sql_query_internal(DBRef, SQuery) of
            {updated, 0} ->
                Count = sql_query_internal(DBRef, ["SELECT count(*) FROM ",Table,";"]),
                case Count of
                     {data, [{"0"}]} ->
                        {updated, _} = sql_query_internal(DBRef, ["DROP VIEW ",view_table(VHost, Schema, Date),";"]),
                        {updated, _} = sql_query_internal(DBRef, ["DROP TABLE ",Table," CASCADE;"]),
                        {updated, _} = sql_query_internal(DBRef, ["LOCK TABLE ",STable," IN ACCESS EXCLUSIVE MODE;"]),
                        {updated, _} = sql_query_internal(DBRef, DQuery),
                        ok;
                     _ ->
                        ?ERROR_MSG("Failed to calculate stats for ~s table! Count was ~p.", [Date, Count]),
                        error
                end;
            {updated, _} ->
                {updated, _} = sql_query_internal(DBRef, ["LOCK TABLE ",STable," IN ACCESS EXCLUSIVE MODE;"]),
                {updated, _} = sql_query_internal(DBRef, ["LOCK TABLE ",TempTable," IN ACCESS EXCLUSIVE MODE;"]),
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
      end, % fun

    case sql_transaction_internal(DBRef, Fun) of
         {atomic, _} ->
            ?INFO_MSG("Rebuilded stats for ~s at ~s", [VHost, Date]),
            ok;
         {aborted, Reason} ->
            ?ERROR_MSG("Failed to rebuild stats for ~s table: ~p.", [Date, Reason]),
            error
    end,
    sql_query_internal(DBRef, ["DROP TABLE ",TempTable,";"]),
    ok.

delete_nonexistent_stats(DBRef, Schema, VHost) ->
    Dates = get_dates_int(DBRef, VHost),
    STable = stats_table(VHost, Schema),

    Temp = lists:flatmap(fun(Date) ->
                             ["'",Date,"'",","]
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

get_user_stats_int(DBRef, Schema, User, VHost) ->
    SName = stats_table(VHost, Schema),
    UName = users_table(VHost, Schema),
    Query = ["SELECT stats.at, sum(stats.count) ",
                 "FROM ",UName," AS users ",
                    "JOIN ",SName," AS stats ON owner_id=user_id "
                 "WHERE users.username='",User,"' ",
                 "GROUP BY stats.at "
                 "ORDER BY DATE(at) DESC;"
             ],
    case sql_query_internal(DBRef, Query) of
         {data, Recs} ->
            {ok, [ {Date, list_to_integer(Count)} || {Date, Count} <- Recs ]};
         {error, Result} ->
            {error, Result}
    end.

delete_all_messages_by_user_at_int(DBRef, Schema, User, VHost, Date) ->
    DQuery = ["DELETE FROM ",messages_table(VHost, Schema, Date)," ",
                 "WHERE owner_id=(SELECT user_id FROM ",users_table(VHost, Schema)," WHERE username='",User,"');"],
    case sql_query_internal(DBRef, DQuery) of
         {updated, _} ->
            ?INFO_MSG("Dropped messages for ~s@~s at ~s", [User, VHost, Date]),
            ok;
         {error, _} ->
            error
    end.

delete_all_stats_by_user_int(DBRef, Schema, User, VHost) ->
    SQuery = ["DELETE FROM ",stats_table(VHost, Schema)," ",
                "WHERE owner_id=(SELECT user_id FROM ",users_table(VHost, Schema)," WHERE username='",User,"');"],
    case sql_query_internal(DBRef, SQuery) of
         {updated, _} ->
             ?INFO_MSG("Dropped all stats for ~s@~s", [User, VHost]),
             ok;
         {error, _} -> error
    end.

delete_stats_by_user_at_int(DBRef, Schema, User, VHost, Date) ->
    SQuery = ["DELETE FROM ",stats_table(VHost, Schema)," ",
                "WHERE owner_id=(SELECT user_id FROM ",users_table(VHost, Schema)," WHERE username='",User,"') ",
                  "AND at='",Date,"';"],
    case sql_query_internal(DBRef, SQuery) of
         {updated, _} ->
             ?INFO_MSG("Dropped stats for ~s@~s at ~s", [User, VHost, Date]),
             ok;
         {error, _} -> error
    end.

delete_user_settings_int(DBRef, Schema, User, VHost) ->
    Query = ["DELETE FROM ",settings_table(VHost, Schema)," ",
                 "WHERE owner_id=(SELECT user_id FROM ",users_table(VHost, Schema)," WHERE username='",User,"');"],
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
create_temp_table(DBRef, VHost, Schema) ->
    TName =  temp_table(VHost, Schema),
    Query = ["CREATE TABLE ",TName," (",
                "owner_id INTEGER, ",
                "peer_name_id INTEGER, ",
                "peer_server_id INTEGER, ",
                "at VARCHAR(20), ",
                "count INTEGER ",
             ");"
            ],
    case sql_query_internal(DBRef, Query) of
         {updated, _} -> ok;
         {error, _Reason} -> error
    end.

create_stats_table(#state{dbref=DBRef, vhost=VHost, schema=Schema}=State) ->
    SName = stats_table(VHost, Schema),

    Fun =
      fun() ->
        Query = ["CREATE TABLE ",SName," (",
                    "owner_id INTEGER, ",
                    "peer_name_id INTEGER, ",
                    "peer_server_id INTEGER, ",
                    "at VARCHAR(20), ",
                    "count integer",
                 ");"
                ],
        case sql_query_internal_silent(DBRef, Query) of
             {updated, _} ->
                {updated, _} = sql_query_internal(DBRef, ["CREATE INDEX \"s_search_i_",Schema,"_",escape_vhost(VHost),"\" ON ",SName," (owner_id, peer_name_id, peer_server_id);"]),
                {updated, _} = sql_query_internal(DBRef, ["CREATE INDEX \"s_at_i_",Schema,"_",escape_vhost(VHost),"\" ON ",SName," (at);"]),
                created;
             {error, Reason} ->
                case lists:keysearch(code, 1, Reason) of
                     {value, {code, "42P07"}} ->
                         exists;
                     _ ->
                         ?ERROR_MSG("Failed to create stats table for ~s: ~p", [VHost, Reason]),
                         error
                end
        end
      end,
    case sql_transaction_internal(DBRef, Fun) of
         {atomic, created} ->
            ?MYDEBUG("Created stats table for ~s", [VHost]),
            rebuild_all_stats_int(State),
            ok;
         {atomic, exists} ->
            ?MYDEBUG("Stats table for ~s already exists", [VHost]),
            {match, [{F, L}]} = re:run(SName, "\".*\""),
            QTable = lists:sublist(SName, F+2, L-2),
            OIDQuery = ["SELECT c.oid FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname='",QTable,"' AND pg_catalog.pg_table_is_visible(c.oid);"],
            {data,[{OID}]} = sql_query_internal(DBRef, OIDQuery),
            CheckQuery = ["SELECT a.attname FROM pg_catalog.pg_attribute a  WHERE a.attrelid = '",OID,"' AND a.attnum > 0 AND NOT a.attisdropped AND a.attname ~ '^peer_.*_id$';"],
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
         {error, _} -> error
    end.

create_settings_table(#state{dbref=DBRef, vhost=VHost, schema=Schema}) ->
    SName = settings_table(VHost, Schema),
    Query = ["CREATE TABLE ",SName," (",
                "owner_id INTEGER PRIMARY KEY, ",
                "dolog_default BOOLEAN, ",
                "dolog_list TEXT DEFAULT '', ",
                "donotlog_list TEXT DEFAULT ''",
             ");"
            ],
    case sql_query_internal_silent(DBRef, Query) of
         {updated, _} ->
            ?MYDEBUG("Created settings table for ~s", [VHost]),
            ok;
         {error, Reason} ->
            case lists:keysearch(code, 1, Reason) of
                 {value, {code, "42P07"}} ->
                   ?MYDEBUG("Settings table for ~s already exists", [VHost]),
                   ok;
                 _ ->
                   ?ERROR_MSG("Failed to create settings table for ~s: ~p", [VHost, Reason]),
                   error
            end
    end.

create_users_table(#state{dbref=DBRef, vhost=VHost, schema=Schema}) ->
    SName = users_table(VHost, Schema),

    Fun =
      fun() ->
        Query = ["CREATE TABLE ",SName," (",
                    "username TEXT UNIQUE, ",
                    "user_id SERIAL PRIMARY KEY",
                 ");"
                ],
        case sql_query_internal_silent(DBRef, Query) of
             {updated, _} ->
                {updated, _} = sql_query_internal(DBRef, ["CREATE INDEX \"username_i_",Schema,"_",escape_vhost(VHost),"\" ON ",SName," (username);"]),
                created;
             {error, Reason} ->
                case lists:keysearch(code, 1, Reason) of
                     {value, {code, "42P07"}} ->
                       exists;
                     _ ->
                       ?ERROR_MSG("Failed to create users table for ~s: ~p", [VHost, Reason]),
                       error
                end
        end
      end,
    case sql_transaction_internal(DBRef, Fun) of
         {atomic, created} ->
             ?MYDEBUG("Created users table for ~s", [VHost]),
             ok;
         {atomic, exists} ->
             ?MYDEBUG("Users table for ~s already exists", [VHost]),
             ok;
         {aborted, _} -> error
    end.

create_servers_table(#state{dbref=DBRef, vhost=VHost, schema=Schema}) ->
    SName = servers_table(VHost, Schema),
    Fun =
      fun() ->
        Query = ["CREATE TABLE ",SName," (",
                    "server TEXT UNIQUE, ",
                    "server_id SERIAL PRIMARY KEY",
                 ");"
                ],
        case sql_query_internal_silent(DBRef, Query) of
             {updated, _} ->
                {updated, _} = sql_query_internal(DBRef, ["CREATE INDEX \"server_i_",Schema,"_",escape_vhost(VHost),"\" ON ",SName," (server);"]),
                created;
             {error, Reason} ->
                case lists:keysearch(code, 1, Reason) of
                     {value, {code, "42P07"}} ->
                       exists;
                     _ ->
                       ?ERROR_MSG("Failed to create servers table for ~s: ~p", [VHost, Reason]),
                       error
                end
        end
      end,
    case sql_transaction_internal(DBRef, Fun) of
         {atomic, created} ->
            ?MYDEBUG("Created servers table for ~s", [VHost]),
            ok;
         {atomic, exists} ->
            ?MYDEBUG("Servers table for ~s already exists", [VHost]),
            ok;
         {aborted, _} -> error
    end.

create_resources_table(#state{dbref=DBRef, vhost=VHost, schema=Schema}) ->
    RName = resources_table(VHost, Schema),
    Fun = fun() ->
            Query = ["CREATE TABLE ",RName," (",
                     "resource TEXT UNIQUE, ",
                     "resource_id SERIAL PRIMARY KEY",
                     ");"
                    ],
            case sql_query_internal_silent(DBRef, Query) of
                 {updated, _} ->
                    {updated, _} = sql_query_internal(DBRef, ["CREATE INDEX \"resource_i_",Schema,"_",escape_vhost(VHost),"\" ON ",RName," (resource);"]),
                    created;
                 {error, Reason} ->
                    case lists:keysearch(code, 1, Reason) of
                         {value, {code, "42P07"}} ->
                           exists;
                         _ ->
                           ?ERROR_MSG("Failed to create users table for ~s: ~p", [VHost, Reason]),
                           error
                    end
            end
          end,
    case sql_transaction_internal(DBRef, Fun) of
         {atomic, created} ->
             ?MYDEBUG("Created resources table for ~s", [VHost]),
             ok;
         {atomic, exists} ->
             ?MYDEBUG("Resources table for ~s already exists", [VHost]),
             ok;
         {aborted, _} -> error
    end.

create_internals(#state{dbref=DBRef, vhost=VHost, schema=Schema}=State) ->
    sql_query_internal(DBRef, ["DROP FUNCTION IF EXISTS ",logmessage_name(VHost,Schema)," (tbname TEXT, atdt TEXT, owner TEXT, peer_name TEXT, peer_server TEXT, peer_resource TEXT, mdirection VARCHAR(4), mtype VARCHAR(9), msubj TEXT, mbody TEXT, mtimestamp DOUBLE PRECISION);"]),
    case sql_query_internal(DBRef, [get_logmessage(VHost, Schema)]) of
         {updated, _} ->
            ?MYDEBUG("Created logmessage for ~p", [VHost]),
            ok;
         {error, Reason} ->
            case lists:keysearch(code, 1, Reason) of
                 {value, {code, "42704"}} ->
                    ?ERROR_MSG("plpgsql language must be installed into database '~s'. Use CREATE LANGUAGE...", [State#state.db]),
                    error;
                 _ ->
                    error
            end
    end.

get_user_id(DBRef, VHost, Schema, User) ->
    SQuery = ["SELECT user_id FROM ",users_table(VHost, Schema)," ",
                 "WHERE username='",User,"';"],
    case sql_query_internal(DBRef, SQuery) of
         {data, []} ->
             IQuery = ["INSERT INTO ",users_table(VHost, Schema)," ",
                          "VALUES ('",User,"');"],
             case sql_query_internal_silent(DBRef, IQuery) of
                  {updated, _} ->
                      {data, [{DBIdNew}]} = sql_query_internal(DBRef, SQuery),
                      DBIdNew;
                  {error, Reason} ->
                      % this can be in clustered environment
                      {value, {code, "23505"}} = lists:keysearch(code, 1, Reason),
                      ?ERROR_MSG("Duplicate key name for ~p", [User]),
                      {data, [{ClID}]} = sql_query_internal(DBRef, SQuery),
                      ClID
             end;
         {data, [{DBId}]} ->
            DBId
    end.

get_logmessage(VHost,Schema) ->
    UName = users_table(VHost,Schema),
    SName = servers_table(VHost,Schema),
    RName = resources_table(VHost,Schema),
    StName = stats_table(VHost,Schema),
    io_lib:format("CREATE OR REPLACE FUNCTION ~s (tbname TEXT, vname TEXT, atdt TEXT, owner TEXT, peer_name TEXT, peer_server TEXT, peer_resource TEXT, mdirection VARCHAR(4), mtype VARCHAR(9), msubj TEXT, mbody TEXT, mtimestamp DOUBLE PRECISION) RETURNS INTEGER AS $$
DECLARE
   ownerID INTEGER;
   peer_nameID INTEGER;
   peer_serverID INTEGER;
   peer_resourceID INTEGER;
   tablename ALIAS for $1;
   viewname ALIAS for $2;
   atdate ALIAS for $3;
BEGIN
   SELECT INTO ownerID user_id FROM ~s WHERE username = owner;
   IF NOT FOUND THEN
      INSERT INTO ~s (username) VALUES (owner);
      ownerID := lastval();
   END IF;

   SELECT INTO peer_nameID user_id FROM ~s WHERE username = peer_name;
   IF NOT FOUND THEN
      INSERT INTO ~s (username) VALUES (peer_name);
      peer_nameID := lastval();
   END IF;

   SELECT INTO peer_serverID server_id FROM ~s WHERE server = peer_server;
   IF NOT FOUND THEN
      INSERT INTO ~s (server) VALUES (peer_server);
      peer_serverID := lastval();
   END IF;

   SELECT INTO peer_resourceID resource_id FROM ~s WHERE resource = peer_resource;
   IF NOT FOUND THEN
      INSERT INTO ~s (resource) VALUES (peer_resource);
      peer_resourceID := lastval();
   END IF;

   BEGIN
      EXECUTE 'INSERT INTO ' || tablename || ' (owner_id, peer_name_id, peer_server_id, peer_resource_id, direction, type, subject, body, timestamp) VALUES (' || ownerID || ',' || peer_nameID || ',' || peer_serverID || ',' || peer_resourceID || ',''' || mdirection || ''',''' || mtype || ''',' || quote_literal(msubj) || ',' || quote_literal(mbody) || ',' || mtimestamp || ')';
   EXCEPTION WHEN undefined_table THEN
      EXECUTE 'CREATE TABLE ' || tablename || ' (' ||
                   'owner_id INTEGER, ' ||
                   'peer_name_id INTEGER, ' ||
                   'peer_server_id INTEGER, ' ||
                   'peer_resource_id INTEGER, ' ||
                   'direction VARCHAR(4) CHECK (direction IN (''to'',''from'')), ' ||
                   'type VARCHAR(9) CHECK (type IN (''chat'',''error'',''groupchat'',''headline'',''normal'')), ' ||
                   'subject TEXT, ' ||
                   'body TEXT, ' ||
                   'timestamp DOUBLE PRECISION)';
      EXECUTE 'CREATE INDEX \"search_i_' || '~s' || '_' || atdate || '_' || '~s' || '\"' || ' ON ' || tablename || ' (owner_id, peer_name_id, peer_server_id, peer_resource_id)';

      EXECUTE 'CREATE OR REPLACE VIEW ' || viewname || ' AS ' ||
                   'SELECT owner.username AS owner_name, ' ||
                          'peer.username AS peer_name, ' ||
                          'servers.server AS peer_server, ' ||
                          'resources.resource AS peer_resource, ' ||
                          'messages.direction, ' ||
                          'messages.type, ' ||
                          'messages.subject, ' ||
                          'messages.body, ' ||
                          'messages.timestamp ' ||
                   'FROM ' ||
                          '~s owner, ' ||
                          '~s peer, ' ||
                          '~s servers, ' ||
                          '~s resources, ' ||
                           tablename || ' messages ' ||
                   'WHERE ' ||
                          'owner.user_id=messages.owner_id and ' ||
                          'peer.user_id=messages.peer_name_id and ' ||
                          'servers.server_id=messages.peer_server_id and ' ||
                          'resources.resource_id=messages.peer_resource_id ' ||
                   'ORDER BY messages.timestamp';

      EXECUTE 'INSERT INTO ' || tablename || ' (owner_id, peer_name_id, peer_server_id, peer_resource_id, direction, type, subject, body, timestamp) VALUES (' || ownerID || ',' || peer_nameID || ',' || peer_serverID || ',' || peer_resourceID || ',''' || mdirection || ''',''' || mtype || ''',' || quote_literal(msubj) || ',' || quote_literal(mbody) || ',' || mtimestamp || ')';
   END;

   UPDATE ~s SET count=count+1 where at=atdate and owner_id=ownerID and peer_name_id=peer_nameID and peer_server_id=peer_serverID;
   IF NOT FOUND THEN
      INSERT INTO ~s (owner_id, peer_name_id, peer_server_id, at, count) VALUES (ownerID, peer_nameID, peer_serverID, atdate, 1);
   END IF;
   RETURN 0;
END;
$$ LANGUAGE plpgsql;
", [logmessage_name(VHost,Schema),UName,UName,UName,UName,SName,SName,RName,RName,Schema,escape_vhost(VHost),UName,UName,SName,RName,StName,StName]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% SQL internals
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% like do_transaction/2 in mysql_conn.erl (changeset by Yariv Sadan <yarivvv@gmail.com>)
sql_transaction_internal(DBRef, Fun) ->
    case sql_query_internal(DBRef, ["BEGIN;"]) of
         {updated, _} ->
            case catch Fun() of
                 error = Err ->
                   rollback_internal(DBRef, Err);
                 {error, _} = Err ->
                   rollback_internal(DBRef, Err);
                 {'EXIT', _} = Err ->
                   rollback_internal(DBRef, Err);
                 Res ->
                   case sql_query_internal(DBRef, ["COMMIT;"]) of
                        {error, _} -> rollback_internal(DBRef, {commit_error});
                        {updated, _} ->
                           case Res of
                                {atomic, _} -> Res;
                                _ -> {atomic, Res}
                           end
                   end
            end;
         {error, _} ->
            {aborted, {begin_error}}
    end.

% like rollback/2 in mysql_conn.erl (changeset by Yariv Sadan <yarivvv@gmail.com>)
rollback_internal(DBRef, Reason) ->
    Res = sql_query_internal(DBRef, ["ROLLBACK;"]),
    {aborted, {Reason, {rollback_result, Res}}}.

sql_query_internal(DBRef, Query) ->
    case sql_query_internal_silent(DBRef, Query) of
         {error, undefined, Rez} ->
            ?ERROR_MSG("Got undefined result: ~p while ~p", [Rez, lists:append(Query)]),
            {error, undefined};
         {error, Error} ->
            ?ERROR_MSG("Failed: ~p while ~p", [Error, lists:append(Query)]),
            {error, Error};
         Rez -> Rez
    end.

sql_query_internal_silent(DBRef, Query) ->
    ?MYDEBUG("DOING: \"~s\"", [lists:append(Query)]),
    % TODO: use pquery?
    get_result(pgsql:squery(DBRef, Query)).

get_result({ok, ["CREATE TABLE"]}) ->
    {updated, 1};
get_result({ok, ["DROP TABLE"]}) ->
    {updated, 1};
get_result({ok, ["ALTER TABLE"]}) ->
    {updated, 1};
get_result({ok,["DROP VIEW"]}) ->
    {updated, 1};
get_result({ok,["DROP FUNCTION"]}) ->
    {updated, 1};
get_result({ok, ["CREATE INDEX"]}) ->
    {updated, 1};
get_result({ok, ["CREATE FUNCTION"]}) ->
    {updated, 1};
get_result({ok, [{[$S, $E, $L, $E, $C, $T, $  | _Rest], _Rows, Recs}]}) ->
    Fun = fun(Rec) ->
              list_to_tuple(
                  lists:map(fun(Elem) when is_binary(Elem) ->
                                  binary_to_list(Elem);
                               (Elem) when is_list(Elem) ->
                                  Elem;
                               (Elem) when is_integer(Elem) ->
                                  integer_to_list(Elem);
                               (Elem) when is_float(Elem) ->
                                  float_to_list(Elem);
                               (Elem) when is_boolean(Elem) ->
                                  atom_to_list(Elem);
                               (Elem) ->
                                  ?ERROR_MSG("Unknown element type ~p", [Elem]),
                                  Elem
                            end, Rec))
          end,
    Res = lists:map(Fun, Recs),
    %{data, [list_to_tuple(Rec) || Rec <- Recs]};
    {data, Res};
get_result({ok, ["INSERT " ++ OIDN]}) ->
    [_OID, N] = string:tokens(OIDN, " "),
    {updated, list_to_integer(N)};
get_result({ok, ["DELETE " ++ N]}) ->
    {updated, list_to_integer(N)};
get_result({ok, ["UPDATE " ++ N]}) ->
    {updated, list_to_integer(N)};
get_result({ok, ["BEGIN"]}) ->
    {updated, 1};
get_result({ok, ["LOCK TABLE"]}) ->
    {updated, 1};
get_result({ok, ["ROLLBACK"]}) ->
    {updated, 1};
get_result({ok, ["COMMIT"]}) ->
    {updated, 1};
get_result({ok, ["SET"]}) ->
    {updated, 1};
get_result({ok, [{error, Error}]}) ->
    {error, Error};
get_result(Rez) ->
    {error, undefined, Rez}.

