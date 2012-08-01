-module(ddfs_tag_test).
-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

% Workaround for Issue 161.
safe_binary() ->
    ?LET(X, list(printable_char()), list_to_binary(X)).

printable_char() ->
    range(32, 126).

token() ->
    oneof([null, safe_binary()]).

user_attr() ->
    list({safe_binary(), safe_binary()}).

tagcontent() ->
    ddfs_tag_util:make_tagcontent(safe_binary(), % id
                                  safe_binary(), % last_modified
                                  token(), % read_token
                                  token(), % write_token
                                  list(safe_binary()), % urls
                                  user_attr()). % user

tag_encode_decode(T) ->
    Encoded = ddfs_tag_util:encode_tagcontent(T),
    {ok, Decoded} = ddfs_tag_util:decode_tagcontent(Encoded),
    case T =:= Decoded of
        true -> ok;
        false -> io:format(" Input=~p~n Output=~p~n", [T, Decoded])
    end,
    T =:= Decoded.

% Sanity check the tag encoding and decoding.
prop_encode_decode() ->
    ?FORALL(T, tagcontent(), tag_encode_decode(T)).

% Ensure that tag date returned as a result of API calls contain the
% appropriate tokens.

token_check(T) ->
    D = ddfs_tag_util:encode_tagcontent_secure(T),
    {struct, J} = mochijson2:decode(D),
    (false =:= lists:keyfind(<<"read-token">>, 1, J))
        andalso (false =:= lists:keyfind(<<"write-token">>, 1, J)).

prop_token_api() ->
    % This does not really need to be a FORALL, just a single instance
    % would do.  It would be nice for triq to support this.  Using
    % eunit here causes two problems:
    % (i) eunit and triq both define LET
    % (ii) there is no simple api to get a single value from a triq
    %      generator for an eunit test.
    ?FORALL(T, tagcontent(), token_check(T)).

do_prop_test() ->
    EunitLeader = erlang:group_leader(),
    erlang:group_leader(whereis(user), self()),
    Res = proper:module(?MODULE),
    erlang:group_leader(EunitLeader, self()),
    ?_assertEqual([], Res).

% Non-property tests.

check_token_test() ->
    % default; backward-compatibility
    write = ddfs_tag_util:check_token(read, null, null, null),
    write = ddfs_tag_util:check_token(write, null, null, null),
    write = ddfs_tag_util:check_token(read, <<"token">>, null, null),
    write = ddfs_tag_util:check_token(write, <<"token">>, null, null),
    % internal clients
    read = ddfs_tag_util:check_token(read, internal, <<"read-token">>, <<"write-token">>),
    write = ddfs_tag_util:check_token(write, internal, <<"read-token">>, <<"write-token">>),
    % read tests
    read = ddfs_tag_util:check_token(read, null, null, <<"write-token">>),
    false = ddfs_tag_util:check_token(read, null, <<"read-token">>, <<"write-token">>),
    read = ddfs_tag_util:check_token(read, <<"read-token">>, <<"read-token">>, <<"write-token">>),
    write = ddfs_tag_util:check_token(read, <<"write-token">>, <<"read-token">>, <<"write-token">>),
    write = ddfs_tag_util:check_token(read, <<"read-token">>, <<"read-token">>, null),
    % write tests
    write = ddfs_tag_util:check_token(write, null, <<"read-token">>, null),
    false = ddfs_tag_util:check_token(write, null, <<"read-token">>, <<"write-token">>),
    false = ddfs_tag_util:check_token(write, <<"read-token">>, <<"read-token">>, <<"write-token">>),
    write = ddfs_tag_util:check_token(read, <<"write-token">>, <<"read-token">>, <<"write-token">>),
    write = ddfs_tag_util:check_token(write, <<"read-token">>, <<"read-token">>, null).
