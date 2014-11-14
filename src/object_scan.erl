#!/usr/bin/env escript

-type key() :: binary().
-type bucket() :: binary() | {binary(), binary()}.

-record(r_object, {
          bucket :: bucket(),
          key :: key(),
          contents :: [#r_content{}],
          vclock = vclock:fresh() :: vclock:vclock(),
          updatemetadata=dict:store(clean, true, dict:new()) :: dict(),
          updatevalue :: term()
         }).

-record(r_content, {
          metadata :: dict(),
          value :: term()
         }).

object_size_pred(Threshold) ->
    fun ({Size, _}) ->
	    Size >= Threshold
    end.

sibling_pred(Threshold) ->
    fun ({_, Obj}) ->
	    length(Obj#r_object.contents) >= Threshold
    end.

modified_before_pred(Threshold) ->
    Last_Modified = last_modified(),
    fun ({_, Obj}) ->
	    Modified = Last_Modified(Obj),
	    riak_core_util:compare_dates(Threshold, Modified)
    end.

any_pred(Preds) ->
    fun (X) ->
	    lists:any(fun (Pred) -> Pred(X) end, Preds)
    end.

last_modified() ->
    fun(Obj) ->
	    lists:foldl(fun (Content, T) ->
				max(dict:fetch(<<"X-Riak-Last-Modified">>,
					       Content#r_content.metadata),
				    T)
			end,
			undefined,
			Obj#r_object.contents)
    end.

object_spec_list() ->
    [{object_size, $o, "object_size", integer, "Fold objects with at least <object_size> bytes"},
     {siblings, $s, "siblings", integer, "Fold objects with at least <siblings> siblings"},
     {modified_before, $B, "modified_before", string, "Fold objects last modified before <modified_before> (using RFC1123)"},
     {cookie, $c, "cookie", string, "Cookie of Riak cluster"},
     {file, $f, "file", string, "Output to <file> on target nodes"}
    ].

process_pred_opt() ->
    fun ({object_size, Threshold}, Preds) ->
	    [object_size_pred(Threshold) | Preds];
	({siblings, Threshold}, Preds) ->
	    [sibling_pred(Threshold) | Preds];
	({modified_before, Threshold}, Preds) ->
	    [modified_before_pred(Threshold) | Preds];
	(_, Preds) -> Preds
    end.

file_fun(File) ->
    fun ({Size, Obj}) ->
	    Key = element(#r_object.key, Obj),
	    Siblings = length(element(#r_object.contents, Obj)),
	    {ok, Dev} = file:open(File, [append]),
	    case element(#r_object.bucket, Obj) of
		{Type, Bucket} ->
		    io:format(Dev,
			      "Object ~s/~s/~s: ~p bytes, ~p siblings~n",
			      [Type, Bucket, Key, Size, Siblings]);
		Bucket ->
		    io:format(Dev,
			      "Object ~s/~s: ~p bytes, ~p siblings~n",
			      [Bucket, Key, Size, Siblings])
	    end,
	    file:close(Dev)
    end.

compose_funs(Funs) ->
    lists:foldl(fun (Fun1, Fun2) ->
			fun (X) -> Fun2(X), Fun1(X) end
		end,
		fun (_) -> ok end,
		Funs).

process_fun_opt() ->
    fun ({file, File}, Funs) ->
	    [file_fun(File) | Funs];
	(_, Funs) -> Funs
    end.

connect(NodeName, Cookie) ->
    {ok, _} = net_kernel:start(['object_fold@127.0.0.1']),
    erlang:set_cookie(node(), list_to_atom(Cookie)),
    true = net_kernel:hidden_connect_node(list_to_atom(NodeName)).

usage() ->
    getopt:usage(object_spec_list(), "object_fold", "<node_name>").

main(Args) ->
    case getopt:parse(object_spec_list(), Args) of
	{ok, {Opts, [NodeName]}} ->
	    Node = list_to_atom(NodeName),
	    Preds = lists:foldl(process_pred_opt(), [], Opts),
	    Pred = any_pred(Preds),
	    Funs = lists:foldl(process_fun_opt(), [], Opts),
	    Fun = compose_funs(Funs),
	    Cookie = proplists:get_value(cookie, Opts, "riak"),
	    VNode_Fold = fold_vnodes(Node),
	    Object_Fold = filter_fold(fold_objects_with_size(), Pred),
	    Fold = map_fold(compose_folds(VNode_Fold, Object_Fold), Fun),
	    connect(NodeName, Cookie),
	    run_fold(Fold);
	{ok, _} ->
	    usage();
	{error, {Reason, Data}} ->
	    io:format("Error: ~s ~p~n", [Reason, Data]),
	    usage()
    end.

fold_vnodes(Node) ->
    fun (Fun, Acc0) ->
	    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_my_ring, []),
	    lists:foldl(Fun, Acc0, riak_core_ring:all_owners(Ring))
    end.

fold_local_vnodes(Node) ->
    filter_fold(fold_vnodes(Node), fun ({_, Host}) -> Host =:= Node end).

fold_kv() ->
    fun (Fun, Acc0, VNode) ->
	    riak_kv_vnode:fold(VNode, fun (KB, Val, Acc) -> Fun({KB, Val}, Acc) end, Acc0)
    end.

fold_objects_with_size() ->
    map_fold(fold_kv(),
	     fun ({{Key, Bucket}, Val}) ->
		     {byte_size(Val), riak_object:from_binary(Bucket, Key, Val)}
	     end).

run_fold(Fold) when is_function(Fold, 2) ->
    Fold(fun (_, {}) -> {} end, {}).

list_fold(Fold) when is_function(Fold, 2) ->
    Fold(fun (X, Xs) -> [X | Xs] end, []).

map_fold(Fold, MapFun) when is_function(Fold, 2) ->
    fun (Fun, Acc0) ->
	    Fold(fun (X, Acc) -> Fun(MapFun(X), Acc) end, Acc0)
    end;
map_fold(Fold, MapFun) when is_function(Fold, 3) ->
    fun (Fun, Acc0, Input) ->
	    Fold(fun (X, Acc) -> Fun(MapFun(X), Acc) end, Acc0, Input)
    end.

filter_fold(Fold, Pred) when is_function(Fold, 2) ->
    fun (Fun, Acc0) ->
	    Fold(fun (X, Acc) -> case Pred(X) of
				     true -> Fun(X, Acc);
				     false -> Acc
				 end end,
		 Acc0)
    end;
filter_fold(Fold, Pred) when is_function(Fold, 3) ->
    fun (Fun, Acc0, Input) ->
	    Fold(fun (X, Acc) -> case Pred(X) of
				     true -> Fun(X, Acc);
				     false -> Acc
				 end end,
		 Acc0, Input)
    end.

compose_folds(Fold1, Fold2) when is_function(Fold1, 2) ->
    fun (Fun, Acc0) ->
	    Fold1(fun (X, Acc) -> Fold2(Fun, Acc, X) end, Acc0)
    end.
