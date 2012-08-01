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
                             P =/= [] andalso
                                 gb_sets:size(gb_sets:from_list(S)) =:= length(P)
                         end),
            ?FORALL(Stage, union(pipeline_utils:stages(P)),
                    begin
                        PG = pipe_graph_new(P),
                        Check = (pipeline_utils:next_stage(P, Stage)
                                 =:= pg_next_stage(PG, Stage)),
                        pipe_graph_delete(PG),
                        Check
                    end)).

do_test() ->
    EunitLeader = erlang:group_leader(),
    erlang:group_leader(whereis(user), self()),
    Res = proper:module(?MODULE),
    erlang:group_leader(EunitLeader, self()),
    ?_assertEqual([], Res).
