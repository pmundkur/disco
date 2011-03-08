-module(disco_worker_util).

-export([jobhome/1,
         make_jobhome/2,
         local_results/2,
         results_filename/1,
         format_output_line/2]).

-include("disco.hrl").

jobhome(JobName) ->
    disco:jobhome(JobName, disco_node:home()).

make_jobhome(JobName, Master) ->
    JobHome = jobhome(JobName),
    case jobpack:extracted(JobHome) of
        true -> JobHome;
        false -> make_jobhome(JobName, JobHome, Master)
    end.
make_jobhome(JobName, JobHome, Master) ->
    JobAtom = list_to_atom(disco:hexhash(JobName)),
    case catch register(JobAtom, self()) of
        true ->
            disco:make_dir(JobHome),
            JobPack = disco_server:get_worker_jobpack(Master, JobName),
            jobpack:save(JobPack, JobHome),
            jobpack:extract(JobPack, JobHome),
            JobHome;
        _Else ->
            wait_for_jobhome(JobAtom, JobName, Master)
    end.
wait_for_jobhome(JobAtom, JobName, Master) ->
    case whereis(JobAtom) of
        undefined ->
            make_jobhome(JobName, Master);
        JobProc ->
            process_flag(trap_exit, true),
            link(JobProc),
            receive
                {'EXIT', noproc} ->
                    make_jobhome(JobName, Master);
                {'EXIT', JobProc, normal} ->
                    make_jobhome(JobName, Master)
            end
    end.

results_filename(Task) ->
    TimeStamp = timer:now_diff(now(), {0,0,0}),
    FileName = io_lib:format("~s-~B-~B.results", [Task#task.mode,
                                                  Task#task.taskid,
                                                  TimeStamp]),
    filename:join(".disco", FileName).

url_path(Task, Host, LocalFile) ->
    LocationPrefix = disco:joburl(Host, Task#task.jobname),
    filename:join(LocationPrefix, LocalFile).

local_results(Task, FileName) ->
    Host = disco:host(node()),
    Output = io_lib:format("dir://~s/~s",
                           [Host, url_path(Task, Host, FileName)]),
    list_to_binary(Output).

format_output_line(Task, [LocalFile, Type]) ->
    format_output_line(Task, [LocalFile, Type, <<"0">>]);
format_output_line(Task, [BLocalFile, Type, Label]) ->
    Host = disco:host(node()),
    LocalFile = binary_to_list(BLocalFile),
    io_lib:format("~s ~s://~s/~s\n",
                  [Label, Type, Host, url_path(Task, Host, LocalFile)]).
