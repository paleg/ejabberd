%%%----------------------------------------------------------------------
%%% File    : gen_logdb.erl
%%% Author  : Oleg Palij (mailto:o.palij@gmail.com)
%%% Purpose : Describes generic behaviour for mod_logdb backends.
%%% Url     : https://paleg.github.io/mod_logdb/
%%%----------------------------------------------------------------------

-module(gen_logdb).
-author('o.palij@gmail.com').

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
   [
    % called from handle_info(start, _)
    % it should logon database and return reference to started instance
    % start(VHost, Opts) -> {ok, SPid} | error
    %  Options - list of options to connect to db
    %    Types: Options = list() -> [] |
    %                              [{user, "logdb"},
    %                               {pass, "1234"},
    %                               {db, "logdb"}] | ...
    %          VHost = list() -> "jabber.example.org"
    {start, 2},

    % called from cleanup/1
    % it should logoff database and do cleanup
    % stop(VHost)
    %    Types: VHost = list() -> "jabber.example.org"
    {stop, 1},

    % called from handle_call({addlog, _}, _, _)
    % it should log messages to database
    % log_message(VHost, Msg) -> ok | error
    %    Types:
    %          VHost = list() -> "jabber.example.org"
    %          Msg = record() -> #msg
    {log_message, 2},

    % called from ejabberdctl rebuild_stats
    % it should rebuild stats table (if used) for vhost
    % rebuild_stats(VHost)
    %    Types:
    %          VHost = list() -> "jabber.example.org"
    {rebuild_stats, 1},

    % it should rebuild stats table (if used) for vhost at Date
    % rebuild_stats_at(VHost, Date)
    %    Types:
    %          VHost = list() -> "jabber.example.org"
    %          Date = list() -> "2007-02-12"
    {rebuild_stats_at, 2},

    % called from user_messages_at_parse_query/5
    % it should delete selected user messages at date
    % delete_messages_by_user_at(VHost, Msgs, Date) -> ok | error
    %    Types:
    %          VHost = list() -> "jabber.example.org"
    %          Msgs = list() -> [ #msg1, msg2, ... ]
    %          Date = list() -> "2007-02-12"
    {delete_messages_by_user_at, 3},

    % called from user_messages_parse_query/4 | vhost_messages_at_parse_query/4
    % it should delete all user messages at date
    % delete_all_messages_by_user_at(User, VHost, Date) -> ok | error
    %    Types:
    %          User = list() -> "admin"
    %          VHost = list() -> "jabber.example.org"
    %          Date = list() -> "2007-02-12"
    {delete_all_messages_by_user_at, 3},

    % called from vhost_messages_parse_query/3
    % it should delete messages for vhost at date and update stats
    % delete_messages_at(VHost, Date) -> ok | error
    %    Types:
    %          VHost = list() -> "jabber.example.org"
    %          Date = list() -> "2007-02-12"
    {delete_messages_at, 2},

    % called from ejabberd_web_admin:vhost_messages_stats/3
    % it should return sorted list of count of messages by dates for vhost
    % get_vhost_stats(VHost) -> {ok, [{Date1, Msgs_count1}, {Date2, Msgs_count2}, ... ]} |
    %                           {error, Reason}
    %    Types:
    %          VHost = list() -> "jabber.example.org"
    %          DateN = list() -> "2007-02-12"
    %          Msgs_countN = number() -> 241
    {get_vhost_stats, 1},

    % called from ejabberd_web_admin:vhost_messages_stats_at/4
    % it should return sorted list of count of messages by users at date for vhost
    % get_vhost_stats_at(VHost, Date) -> {ok, [{User1, Msgs_count1}, {User2, Msgs_count2}, ....]} |
    %                                    {error, Reason}
    %    Types:
    %          VHost = list() -> "jabber.example.org"
    %          Date = list() -> "2007-02-12"
    %          UserN = list() -> "admin"
    %          Msgs_countN = number() -> 241
    {get_vhost_stats_at, 2},

    % called from ejabberd_web_admin:user_messages_stats/4
    % it should return sorted list of count of messages by date for user at vhost
    % get_user_stats(User, VHost) -> {ok, [{Date1, Msgs_count1}, {Date2, Msgs_count2}, ...]} |
    %                                {error, Reason}
    %    Types:
    %          User = list() -> "admin"
    %          VHost = list() -> "jabber.example.org"
    %          DateN = list() -> "2007-02-12"
    %          Msgs_countN = number() -> 241
    {get_user_stats, 2},

    % called from ejabberd_web_admin:user_messages_stats_at/5
    % it should return all user messages at date
    % get_user_messages_at(User, VHost, Date) -> {ok, Msgs} | {error, Reason}
    %    Types:
    %          User = list() -> "admin"
    %          VHost = list() -> "jabber.example.org"
    %          Date = list() -> "2007-02-12"
    %          Msgs = list() -> [ #msg1, msg2, ... ]
    {get_user_messages_at, 3},

    % called from many places
    % it should return list of dates for vhost
    % get_dates(VHost) -> [Date1, Date2, ... ]
    %    Types:
    %          VHost = list() -> "jabber.example.org"
    %          DateN = list() -> "2007-02-12"
    {get_dates, 1},

    % called from start
    % it should return list with users settings for VHost in db
    % get_users_settings(VHost) -> [#user_settings1, #user_settings2, ... ] | error
    %    Types:
    %          VHost = list() -> "jabber.example.org"
    {get_users_settings, 1},

    % called from many places
    % it should return User settings at VHost from db
    % get_user_settings(User, VHost) -> error | {ok, #user_settings}
    %    Types:
    %          User = list() -> "admin"
    %          VHost = list() -> "jabber.example.org"
    {get_user_settings, 2},

    % called from web admin
    % it should set User settings at VHost
    % set_user_settings(User, VHost, #user_settings) -> ok | error
    %    Types:
    %          User = list() -> "admin"
    %          VHost = list() -> "jabber.example.org"
    {set_user_settings, 3},

    % called from remove_user (ejabberd hook)
    % it should remove user messages and settings at VHost
    % drop_user(User, VHost) -> ok | error
    %    Types:
    %          User = list() -> "admin"
    %          VHost = list() -> "jabber.example.org"
    {drop_user, 2}
   ];
behaviour_info(_) ->
   undefined.
