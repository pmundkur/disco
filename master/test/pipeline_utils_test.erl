-module(pipeline_utils_test).
-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("common_types.hrl").
-include("disco.hrl").
-include("pipeline.hrl").

pipe_graph_new(Pipeline) ->
    pipe_graph(digraph:new([acyclic]), Pipeline).
pipe_graph(G, []) ->
    G;
pipe_graph(G, [{H, _HG} | []]) ->
    _HV = digraph:add_vertex(G, H),
    G;
pipe_graph(G, [{H, _HG} | [{T, TG} | _] = Rest]) ->
    HV = digraph:add_vertex(G, H),
    TV = digraph:add_vertex(G, T),
    digraph:add_edge(G, HV, TV, TG),
    pipe_graph(G, Rest).

pipe_graph_delete(G) -> digraph:delete(G).

pg_next_stage(G, V) ->
    case digraph:out_edges(G, V) of
        [] ->
            done;
        [NE | []] ->
            {NE, V, N, NG} = digraph:edge(G, NE),
            {N, NG}
    end.

prop_next_stage() ->
    ?FORALL(P, ?SUCHTHAT(P, pipeline(),
                         begin
                             S = pipeline_utils:stages(P),
                             NUniqS = gb_sets:size(gb_sets:from_list(S)),
                             P =/= [] andalso NUniqS =:= length(P)
                         end),
            ?FORALL(Stage, union(pipeline_utils:stages(P)),
                    begin
                        PG = pipe_graph_new(P),
                        Check = (pipeline_utils:next_stage(P, Stage)
                                 =:= pg_next_stage(PG, Stage)),
                        pipe_graph_delete(PG),
                        Check
                    end)).

% Grouped outputs should be in distinct groups, except for split,
% which generates one, possibly non-unique, group per useful input.

useful_input({data, {_L, _S, []}}) -> false;
useful_input({dir, {_H, _U, []}})  -> false;
useful_input(_) -> true.

prop_distinct_groups() ->
    ?FORALL({LG, TOs}, {label_grouping(), [{task_id(), [task_output()]}]},
            begin
                GOs = pipeline_utils:group_outputs(LG, TOs),
                Groups = [G || {G, _O} <- GOs],
                UOs = [O || {_Tid, Tout} <- TOs,
                            {_Outid, O} <- Tout,
                            useful_input(O)],
                NUniqG = gb_sets:size(gb_sets:from_list(Groups)),
                (LG =:= split andalso length(UOs) =:= length(GOs))
                    orelse length(Groups) =:= NUniqG
            end).

% When joining by label, grouped outputs should have the group's label.

have_label(_L, []) -> true;
have_label(L, [{_Iid, {data, {OL, _S, _Reps}}} | Rest]) ->
    L =:= OL andalso have_label(L, Rest);
have_label(L, [{_Iid, {dir, {_H, _U, LS}}} | Rest]) ->
    lists:member(L, [LL || {LL, _Sz} <- LS]) andalso have_label(L, Rest).

prop_group_labels() ->
    ?FORALL({LG, TOs}, {union([join_label, join_node_label]),
                        [{task_id(), [task_output()]}]},
            begin
                GOs = pipeline_utils:group_outputs(LG, TOs),
                lists:all(fun({{L, _}, Os}) -> have_label(L, Os) end, GOs)
            end).

% When joining by node, grouped outputs should be on the group's node.

on_node(_H, []) -> true;
on_node(H, [{_Iid, {data, {_L, _S, Reps}}} | Rest]) ->
    lists:member(H, [HH || {_Url, HH} <- Reps]) andalso on_node(H, Rest);
on_node(H, [{_Iid, {dir, {OH, _U, _Ls}}} | Rest]) ->
    H =:= OH andalso on_node(H, Rest).

prop_group_nodes() ->
    ?FORALL({LG, TOs}, {union([join_node, join_node_label]),
                        [{task_id(), [task_output()]}]},
            begin
                GOs = pipeline_utils:group_outputs(LG, TOs),
                lists:all(fun({{_L, H}, Os}) -> on_node(H, Os) end, GOs)
            end).

% When joining all, ensure all useful inputs are present in output.

prop_join_all() ->
    ?FORALL(TOs, [{task_id(), [task_output()]}],
            begin
                [{{0, none}, Os}] = pipeline_utils:group_outputs(join_all, TOs),
                UOs = [{{Tid, Outid}, O} || {Tid, Tout} <- TOs,
                                            {Outid, O} <- Tout,
                                            useful_input(O)],
                [] =:= (UOs -- Os)
            end).

-define(NUM_PROPER_ITERS, 500).

props() ->
    EunitLeader = erlang:group_leader(),
    erlang:group_leader(whereis(user), self()),
    Res = proper:module(?MODULE, [{numtests, ?NUM_PROPER_ITERS}]),
    erlang:group_leader(EunitLeader, self()),
    ?assertEqual([], Res).

% Default eunit test timeout is 5 seconds, which is exceeded if we use
% a large numtests for PropEr.

do_test_() ->
    {timeout, 5 + (?NUM_PROPER_ITERS/100), [fun props/0]}.
