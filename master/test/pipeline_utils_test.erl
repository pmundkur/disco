-module(pipeline_utils_test).

-include_lib("proper/include/proper.hrl").

-include("common_types.hrl").
-include("disco.hrl").
-include("pipeline.hrl").

pipe_graph(Pipeline) ->
    pipe_graph(digraph:new([acyclic]), pipeline_utils:stages(Pipeline)).
pipe_graph(G, []) ->
    G;
pipe_graph(G, [H | []]) ->
    HV = digraph:add_vertex(G, H),
    TV = digraph:add_vertex(G, done),
    digraph:add_edge(G, HV, TV),
    G;
pipe_graph(G, [H | [T|_] = Rest]) ->
    HV = digraph:add_vertex(G, H),
    TV = digraph:add_vertex(G, T),
    digraph:add_edge(G, HV, TV),
    pipe_graph(G, Rest).

pg_next_stage(G, V) ->
    case digraph:out_neighbours(G, V) of
        [N] -> N;
        _   -> false
    end.

prop_next_stage() ->
    ?FORALL(Pipeline, pipeline(),
            ?FORALL(Stage, proper:union(pipeline_utils:stages(Pipeline)),
                    begin
                        PG = pipe_graph(Pipeline),
                        pipeline_utils:next_stage(Pipeline, Stage) =:= pg_next_stage(PG, Stage)
                    end)).

prop_test() ->
    proper:quickcheck(prop_next_stage()).
