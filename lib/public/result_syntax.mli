val ( let* ) : ('a, 'error) result -> ('a -> ('b, 'error) result) -> ('b, 'error) result
val ( let+ ) : ('a, 'error) result -> ('a -> 'b) -> ('b, 'error) result
