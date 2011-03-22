-module(ddfs_node).
-behaviour(gen_server).

-export([start/1, start_link/1]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("config.hrl").

% Diskinfo is {FreeSpace, UsedSpace}.
-type diskinfo() :: {non_neg_integer(), non_neg_integer()}.
-record(state, {root :: nonempty_string(),
                initproc :: 'undefined' | pid(),
                vols :: 'undefined' | [{diskinfo(), nonempty_string()},...],
                reqq :: [_],
                putq :: http_queue:q(),
                getq :: http_queue:q(),
                tags :: 'undefined' | gb_tree()}).

is_master() ->
    case net_adm:names() of
        {ok, Names} ->
            lists:keymember(disco:master_name(), 1, Names);
        {error, address} ->
            false
    end.

start(Master) ->
    case is_master() of
        true ->
            % the master handles GET requests specially using a dummy ddfs_node
            spawn_link(Master, ?MODULE, start_link, [{false, false}]),
            spawn_link(?MODULE, start_link, [{false, true}]);
        false ->
            spawn_link(?MODULE, start_link, [{true, true}])
    end.

start_link(Config) ->
    error_logger:info_report({"DDFS node starts"}),
    case gen_server:start_link({local, ?MODULE},
                               ?MODULE,
                               Config,
                               [{timeout, ?NODE_STARTUP}]) of
        {ok, _Pid} ->
            ok;
        {error, Reason} ->
            exit(Reason)
    end.

async_init(DDFSNode, DDFSRoot, GetEnabled, PutEnabled) ->
    {ok, Vols} = find_vols(DDFSRoot),
    {ok, Tags} = find_tags(DDFSRoot, Vols),
    gen_server:cast(DDFSNode, {init_done, DDFSRoot, Vols, Tags, GetEnabled, PutEnabled}).

init({GetEnabled, PutEnabled}) ->
    DDFSRoot = disco:get_setting("DDFS_ROOT"),
    PutMax = list_to_integer(disco:get_setting("DDFS_PUT_MAX")),
    GetMax = list_to_integer(disco:get_setting("DDFS_GET_MAX")),
    Self = self(),
    InitProc = spawn_link(fun() ->
                              async_init(Self, DDFSRoot, GetEnabled, PutEnabled)
                          end),
    {ok, #state{root = DDFSRoot,
                initproc = InitProc,
                vols = undefined,
                tags = undefined,
                reqq = [],
                putq = http_queue:new(PutMax, ?HTTP_QUEUE_LENGTH),
                getq = http_queue:new(GetMax, ?HTTP_QUEUE_LENGTH)}}.

% Queue up calls until initialization is complete.
handle_call(Req, From, #state{vols = undefined, reqq = ReqQ} = S) ->
    {noreply, S#state{reqq = [{Req, From} | ReqQ]}};

% NOTE: If you add a _call_ below, ensure that you handle it in
% dispatch_pending_request() as well.

handle_call(get_tags, _, S) ->
    {reply, do_get_tags(S), S};

handle_call(get_vols, _, S) ->
    {reply, do_get_vols(S), S};

handle_call(get_blob, From, S) ->
    do_get_blob(From, S);

handle_call(get_diskspace, _From, S) ->
    {reply, do_get_diskspace(S), S};

handle_call({put_blob, BlobName}, From, S) ->
    do_put_blob(BlobName, From, S);

handle_call({get_tag_timestamp, TagName}, _From, S) ->
    {reply, do_get_tag_timestamp(TagName, S), S};

handle_call({get_tag_data, Tag, {_Time, VolName}}, From, S) ->
    spawn(fun() -> do_get_tag_data(Tag, VolName, From, S) end),
    {noreply, S};

handle_call({put_tag_data, {Tag, Data}}, _From, S) ->
    {reply, do_put_tag_data(Tag, Data, S), S};

handle_call({put_tag_commit, Tag, TagVol}, _, S) ->
    {Reply, S1} = do_put_tag_commit(Tag, TagVol, S),
    {reply, Reply, S1}.


handle_cast({init_done, DDFSRoot, Vols, Tags, GetEnabled, PutEnabled}, S) ->
    {noreply, do_init_done(DDFSRoot, Vols, Tags, GetEnabled, PutEnabled, S)};

% Ignore other casts received during initialization.
handle_cast(_Req, #state{vols = undefined} = S) ->
    {noreply, S};

handle_cast({update_vols, NewVols}, #state{vols = Vols} = S) ->
    {noreply, S#state{vols = lists:ukeymerge(2, NewVols, Vols)}};

handle_cast({update_tags, Tags}, S) ->
    {noreply, S#state{tags = Tags}}.

handle_info({'EXIT', From, Reason}, #state{initproc = From} = S) ->
    error_logger:error_report({"ddfs_node init failed!", Reason}),
    {stop, init_failed, S};

handle_info({'EXIT', From, Reason}, S) ->
    {noreply, S};

handle_info({'DOWN', _, _, Pid, _}, #state{putq = PutQ, getq = GetQ} = S) ->
    % We don't know if Pid refers to a put or get request.
    % We can safely try to remove it both the queues: It can exist in
    % one of the queues at most.
    {_, NewPutQ} = http_queue:remove(Pid, PutQ),
    {_, NewGetQ} = http_queue:remove(Pid, GetQ),
    {noreply, S#state{putq = NewPutQ, getq = NewGetQ}}.

% callback stubs
terminate(_Reason, _State) ->
    {}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ===================================================================
%% internal functions

do_init_done(DDFSRoot, Vols, Tags, GetEnabled, PutEnabled, S) ->
    DiscoRoot = disco:get_setting("DISCO_DATA"),
    PutPort = list_to_integer(disco:get_setting("DDFS_PUT_PORT")),
    GetPort = list_to_integer(disco:get_setting("DISCO_PORT")),

    if
        GetEnabled ->
            {ok, _GetPid} = ddfs_get:start([{port, GetPort}],
                                           {DDFSRoot, DiscoRoot});
        true ->
            ok
    end,
    if
        PutEnabled ->
            {ok, _PutPid} = ddfs_put:start([{port, PutPort}]);
        true ->
            ok
    end,

    spawn_link(fun() -> refresh_tags(DDFSRoot, Vols) end),
    spawn_link(fun() -> monitor_diskspace(DDFSRoot, Vols) end),

    dispatch_pending(S#state{vols = Vols, tags = Tags, initproc = undefined}).

do_get_tags(#state{tags = Tags}) ->
    gb_trees:keys(Tags).

do_get_vols(#state{vols = Vols, root = Root}) ->
    {Vols, Root}.

do_get_blob({Pid, _Ref} = From, #state{getq = Q} = S) ->
    Reply = fun() -> gen_server:reply(From, ok) end,
    case http_queue:add({Pid, Reply}, Q) of
        full ->
            {reply, full, S};
        {_, NewQ} ->
            erlang:monitor(process, Pid),
            {noreply, S#state{getq = NewQ}}
    end.

do_get_diskspace(#state{vols = Vols}) ->
    lists:foldl(fun ({{Free, Used}, _VolName}, {TotalFree, TotalUsed}) ->
                        {TotalFree + Free, TotalUsed + Used}
                end, {0, 0}, Vols).

do_put_blob(BlobName, {Pid, _Ref} = From, #state{putq = Q} = S) ->
    Reply = fun() ->
                    {_Space, VolName} = choose_vol(S#state.vols),
                    {ok, Local, Url} = ddfs_util:hashdir(list_to_binary(BlobName),
                                                         disco:host(node()),
                                                         "blob",
                                                         S#state.root,
                                                         VolName),
                    case ddfs_util:ensure_dir(Local) of
                        ok ->
                            gen_server:reply(From, {ok, Local, Url});
                        {error, E} ->
                            gen_server:reply(From, {error, Local, E})
                    end
            end,
    case http_queue:add({Pid, Reply}, Q) of
        full ->
            {reply, full, S};
        {_, NewQ} ->
            erlang:monitor(process, Pid),
            {noreply, S#state{putq = NewQ}}
    end.

do_get_tag_timestamp(TagName, S) ->
    case gb_trees:lookup(TagName, S#state.tags) of
        none ->
            notfound;
        {value, {_Time, _VolName} = TagNfo} ->
            {ok, TagNfo}
    end.

do_get_tag_data(Tag, VolName, From, S) ->
    {ok, TagDir, _Url} = ddfs_util:hashdir(Tag,
                                           disco:host(node()),
                                           "tag",
                                           S#state.root,
                                           VolName),
    TagPath = filename:join(TagDir, binary_to_list(Tag)),
    case prim_file:read_file(TagPath) of
        {ok, Binary} ->
            gen_server:reply(From, {ok, Binary});
        {error, Reason} ->
            error_logger:warning_report({"Read failed", TagPath, Reason}),
            gen_server:reply(From, {error, read_failed})
    end.

do_put_tag_data(Tag, Data, S) ->
    {_Space, VolName} = choose_vol(S#state.vols),
    {ok, Local, _} = ddfs_util:hashdir(Tag,
                                       disco:host(node()),
                                       "tag",
                                       S#state.root,
                                       VolName),
    case ddfs_util:ensure_dir(Local) of
        ok ->
            Filename = filename:join(Local, ["!partial.", binary_to_list(Tag)]),
            case prim_file:write_file(Filename, Data) of
                ok ->
                    {ok, VolName};
                {error, _} = E ->
                    E
            end;
        E ->
            E
    end.

do_put_tag_commit(Tag, TagVol, S) ->
    {value, {_, VolName}} = lists:keysearch(node(), 1, TagVol),
    {ok, Local, Url} = ddfs_util:hashdir(Tag,
                                         disco:host(node()),
                                         "tag",
                                         S#state.root,
                                         VolName),
    {TagName, Time} = ddfs_util:unpack_objname(Tag),

    TagL = binary_to_list(Tag),
    Src = filename:join(Local, ["!partial.", TagL]),
    Dst = filename:join(Local,  TagL),
    case ddfs_util:safe_rename(Src, Dst) of
        ok ->
            {{ok, Url},
             S#state{tags = gb_trees:enter(TagName,
                                           {Time, VolName},
                                           S#state.tags)}};
        {error, _} = E ->
            {E, S}
    end.

dispatch_pending(#state{reqq = []} = S) ->
    S;
dispatch_pending(#state{reqq = [{Req, From} | Rest]} = S) ->
    S1 = dispatch_pending_request(Req, From, S),
    dispatch_pending(S1#state{reqq = Rest}).

dispatch_pending_request(get_tags, From, S) ->
    gen_server:reply(From, do_get_tags(S)),
    S;
dispatch_pending_request(get_vols, From, S) ->
    gen_server:reply(From, do_get_vols(S)),
    S;
dispatch_pending_request(get_blob, From, S) ->
    case do_get_blob(From, S) of
        {reply, Reply, S1} ->
            gen_server:reply(From, Reply),
            S1;
        {noreply, S1} ->
            S1
    end;
dispatch_pending_request(get_diskspace, From, S) ->
    gen_server:reply(From, do_get_diskspace(S)),
    S;
dispatch_pending_request({put_blob, BlobName}, From, S) ->
    case do_put_blob(BlobName, From, S) of
        {reply, Reply, S1} ->
            gen_server:reply(From, Reply),
            S1;
        {noreply, S1} ->
            S1
    end;
dispatch_pending_request({get_tag_timestamp, TagName}, From, S) ->
    gen_server:reply(From, do_get_tag_timestamp(TagName, S)),
    S;
dispatch_pending_request({get_tag_data, Tag, {_Time, VolName}}, From, S) ->
    spawn(fun() -> do_get_tag_data(Tag, VolName, From, S) end),
    S;
dispatch_pending_request({put_tag_data, {Tag, Data}}, From, S) ->
    gen_server:reply(From, do_put_tag_data(Tag, Data, S)),
    S;
dispatch_pending_request({put_tag_commit, Tag, TagVol}, From, S) ->
    {Reply, S1} = do_put_tag_commit(Tag, TagVol, S),
    gen_server:reply(From, Reply),
    S1.

-spec init_vols(nonempty_string(), [nonempty_string(),...]) ->
    {'ok', [{diskinfo(), nonempty_string()},...]}.
init_vols(Root, VolNames) ->
    lists:foreach(fun(VolName) ->
                          prim_file:make_dir(filename:join([Root, VolName, "blob"])),
                          prim_file:make_dir(filename:join([Root, VolName, "tag"]))
                  end, VolNames),
    {ok, [{{0, 0}, VolName} || VolName <- lists:sort(VolNames)]}.

-spec find_vols(nonempty_string()) ->
    'eof' | 'ok' | {'ok', [{diskinfo(), nonempty_string()}]} | {'error', _}.
find_vols(Root) ->
    case prim_file:list_dir(Root) of
        {ok, Files} ->
            case [F || "vol" ++ _ = F <- Files] of
                [] ->
                    VolName = "vol0",
                    prim_file:make_dir(filename:join([Root, VolName])),
                    error_logger:warning_report({"Could not find volumes in ", Root,
                                                 "Created ", VolName}),
                    init_vols(Root, [VolName]);
                VolNames ->
                    init_vols(Root, VolNames)
            end;
        Error ->
            error_logger:warning_report(
                {"Invalid root directory", Root, Error}),
            Error
    end.

-spec find_tags(nonempty_string(), [{diskinfo(), nonempty_string()},...]) ->
    {'ok', gb_tree()}.
find_tags(Root, Vols) ->
    {ok,
     lists:foldl(fun({_Space, VolName}, Tags) ->
                         ddfs_util:fold_files(filename:join([Root, VolName, "tag"]),
                                              fun(Tag, _, Tags1) ->
                                                      parse_tag(Tag, VolName, Tags1)
                                              end, Tags)
                 end, gb_trees:empty(), Vols)}.

-spec parse_tag(nonempty_string(), nonempty_string(), gb_tree()) -> gb_tree().
parse_tag("!" ++ _, _, Tags) -> Tags;
parse_tag(Tag, VolName, Tags) ->
    {TagName, Time} = ddfs_util:unpack_objname(Tag),
    case gb_trees:lookup(TagName, Tags) of
        none ->
            gb_trees:insert(TagName, {Time, VolName}, Tags);
        {value, {OTime, _}} when OTime < Time ->
            gb_trees:enter(TagName, {Time, VolName}, Tags);
        _ ->
            Tags
    end.

-spec choose_vol([{diskinfo(), nonempty_string()},...]) ->
    {diskinfo(), nonempty_string()}.
choose_vol(Vols) ->
    % Choose the volume with most available space.  Note that the key
    % being sorted is the diskinfo() tuple, which has free-space as
    % the first element.
    [Vol|_] = lists:reverse(lists:keysort(1, Vols)),
    Vol.

-spec monitor_diskspace(nonempty_string(),
                        [{diskinfo(), nonempty_string()},...]) ->
    no_return().
monitor_diskspace(Root, Vols) ->
    timer:sleep(?DISKSPACE_INTERVAL),
    Df = fun(VolName) ->
            ddfs_util:diskspace(filename:join([Root, VolName]))
         end,
    NewVols = [{Space, VolName}
               || {VolName, {ok, Space}}
               <- [{VolName, Df(VolName)} || {_OldSpace, VolName} <- Vols]],
    gen_server:cast(ddfs_node, {update_vols, NewVols}),
    monitor_diskspace(Root, NewVols).

-spec refresh_tags(nonempty_string(), [{diskinfo(), nonempty_string()},...]) ->
    no_return().
refresh_tags(Root, Vols) ->
    timer:sleep(?FIND_TAGS_INTERVAL),
    {ok, Tags} = find_tags(Root, Vols),
    gen_server:cast(ddfs_node, {update_tags, Tags}),
    refresh_tags(Root, Vols).
