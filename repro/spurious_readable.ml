(* Counts Unix.wait_readable wakeups after which the fd is not readable.

   Pairs of fibers bounce one byte over two pipes: each waits until its
   pipe is readable, reads the byte with a non-blocking read and writes it
   to the other pipe. Only one byte is ever in flight, and each pipe has a
   single reader, so a read that finds nothing after a wakeup means the
   wakeup was spurious.

   Usage: spurious_readable.exe [DOMAINS] [PAIRS] [ROUNDS] *)

open Affect
module Aunix = Affect_unix.Unix

let arg i default =
  if Array.length Sys.argv > i then int_of_string Sys.argv.(i) else default

let () =
  let domains = arg 1 4 and pairs = arg 2 8 and rounds = arg 3 20_000 in
  let spurious = Atomic.make 0 in
  let pipe () =
    let r, w = Unix.pipe ~cloexec:true () in
    Unix.set_nonblock r; Unix.set_nonblock w; r, w
  in
  let rec read_byte fd buf =
    Action.invoke (Aunix.wait_readable fd ());
    match Unix.read fd buf 0 1 with
    | 1 -> ()
    | _ -> failwith "unexpected EOF"
    | exception Unix.Unix_error ((EAGAIN | EWOULDBLOCK), _, _) ->
        Atomic.incr spurious; read_byte fd buf
  in
  let player ~first ~input ~output () =
    let buf = Bytes.make 1 'x' in
    if first then ignore (Unix.write output buf 0 1);
    for _ = 1 to rounds do
      read_byte input buf;
      ignore (Unix.write output buf 0 1)
    done
  in
  Aunix.main ~domain_count:domains (fun () ->
      let pair () =
        let a_in, b_out = pipe () and b_in, a_out = pipe () in
        [ Fun.Async.call (player ~first:true ~input:a_in ~output:a_out);
          Fun.Async.call (player ~first:false ~input:b_in ~output:b_out) ]
      in
      let calls = List.concat (List.init pairs (fun _ -> pair ())) in
      List.iter Fun.Async.get calls);
  Printf.printf "domains=%d pairs=%d rounds=%d: %d spurious wakeups\n%!"
    domains pairs rounds (Atomic.get spurious)
