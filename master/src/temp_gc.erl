-module(temp_gc).

-include_lib("kernel/include/file.hrl").

-export([start_link/1]).

-define(GC_INTERVAL, 600000).

-spec start_link(pid()) -> no_return().
start_link(Master) ->
    case catch register(temp_gc, self()) of
        {'EXIT', {badarg, _}} ->
            exit(already_started);
        _ -> ok
    end,
    put(master, Master),
    loop().

-spec loop() -> no_return().
loop() ->
    case catch {get_purged(), get_jobs()} of
        {{ok, Purged}, {ok, Jobs}} ->
            DataRoot = disco:data_root(node()),
            case prim_file:list_dir(DataRoot) of
                {ok, Dirs} ->
                    Active = gb_sets:from_list(
                        [Name || {Name, active, _Start, _Pid} <- Jobs]),
                    GCAfter = list_to_integer(disco:get_setting("DISCO_GC_AFTER")),
                    error_logger:warning_report({"temp_gc: starting", DataRoot, GCAfter}),
                    process_dir(Dirs, gb_sets:from_ordset(Purged), Active),
                    error_logger:warning_report({"temp_gc: done"});
                Other ->
                    error_logger:warning_report({"temp_gc: ignoring DataRoot listing", DataRoot, Other}),
                    % fresh install, try again after GC_INTERVAL
                    ok
            end;
        Other ->
            error_logger:warning_report({"temp_gc: ignoring unexpected master response", Other}),
            % master busy, try again after GC_INTERVAL
            ok
    end,
    timer:sleep(?GC_INTERVAL),
    flush(),
    loop().

% gen_server calls below may timeout, so we need to purge late replies
flush() ->
    receive
        _ ->
            flush()
    after 0 ->
        ok
    end.

ddfs_delete(Tag) ->
    ddfs:delete({ddfs_master, get(master)}, Tag, internal).

get_purged() ->
    gen_server:call({disco_server, get(master)}, get_purged).

get_jobs() ->
    gen_server:call({event_server, get(master)}, get_jobs).

-spec process_dir([string()], gb_set(), gb_set()) -> 'ok'.
process_dir([], _Purged, _Active) -> ok;
process_dir([Dir|R], Purged, Active) ->
    Path = disco:data_path(node(), Dir),
    error_logger:warning_report({"temp_gc: processing", Path}),
    {ok, Jobs} = prim_file:list_dir(Path),
    _ = [process_job(filename:join(Path, Job), Purged) ||
            Job <- Jobs, ifdead(Job, Active)],
    process_dir(R, Purged, Active).

-spec ifdead(string(), gb_set()) -> bool().
ifdead(Job, Active) ->
    not gb_sets:is_member(list_to_binary(Job), Active).

-spec process_job(string(), gb_set()) -> 'ok'.
process_job(JobPath, Purged) ->
    case prim_file:read_file_info(JobPath) of
        {ok, #file_info{type = directory, mtime = TStamp}} ->
            T = calendar:datetime_to_gregorian_seconds(TStamp),
            Now = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
            Job = filename:basename(JobPath),
            IsPurged = gb_sets:is_member(list_to_binary(Job), Purged),
            GCAfter = list_to_integer(disco:get_setting("DISCO_GC_AFTER")),
            if IsPurged; Now - T > GCAfter ->
                ddfs_delete(disco:oob_name(Job)),
                error_logger:warning_report({"temp_gc: cleaning jobdir", JobPath}),
                _ = os:cmd("rm -Rf " ++ JobPath),
                ok;
            true ->
                error_logger:warning_report({"temp_gc: skipping unpurged/recent job", JobPath,
                                             IsPurged, Now - T, GCAfter}),
                ok
            end;
        Err ->
            error_logger:warning_report({"temp_gc: error processing jobdir", JobPath, Err}),
            ok
    end.
