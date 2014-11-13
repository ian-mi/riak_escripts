-module(vnode_fold).
-export([fold_vnodes/2,
	 fold_local_vnodes/2,
	 fold_kv/3,
	 fold_local_vnodes_kv/2,
	 fold_local_vnodes_kv_verbose/2,
	 list_fold/1,
	 map_fold/2,
	 filter_fold/2,
	 compose_folds/2]).

fold_vnodes(Fun, Acc0) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    lists:foldl(Fun, Acc0, riak_core_ring:all_owners(Ring)).	       

fold_local_vnodes(Fun, Acc0) ->
    Fold = filter_fold(fun ?MODULE:fold_vnodes/2, fun ({_, Host}) -> Host =:= node() end),
    Fold(Fun, Acc0).

fold_kv(Fun, Acc0, VNode) ->
    riak_kv_vnode:fold(VNode, fun (KB, Val, Acc) -> Fun({KB, Val}, Acc) end, Acc0).

fold_local_vnodes_kv(Fun, Acc0) ->
    Fold = compose_folds(fun ?MODULE:fold_local_vnodes/2, fun ?MODULE:fold_kv/3),
    Fold(Fun, Acc0).

fold_local_vnodes_kv_verbose(Fun, Acc0) ->
    Fold = compose_folds(
	     map_fold(fun ?MODULE:fold_local_vnodes/2,
		      fun (VNode={P,_}) -> io:format("Folding vnode ~p~n", [P]), VNode end),
	     fun ?MODULE:fold_kv/3),
    Fold(Fun, Acc0).

list_fold(Fold) when is_function(Fold, 2) ->
    Fold(fun (X, Xs) -> [X | Xs] end, []).

map_fold(Fold, MapFun) when is_function(Fold, 2) ->
    fun (Fun, Acc0) ->
	    Fold(fun (X, Acc) -> Fun(MapFun(X), Acc) end, Acc0)
    end.

filter_fold(Fold, Pred) when is_function(Fold, 2) ->
    fun (Fun, Acc0) ->
	    Fold(fun (X, Acc) -> case Pred(X) of true -> Fun(X, Acc); false -> Acc end end, Acc0)
    end.

compose_folds(Fold1, Fold2) when is_function(Fold1, 2) ->
    fun (Fun, Acc0) ->
	    Fold1(fun (X, Acc) -> Fold2(Fun, Acc, X) end, Acc0)
    end.
