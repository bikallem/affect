(*---------------------------------------------------------------------------
   Copyright (c) 2026 The affect programmers. All rights reserved.
   SPDX-License-Identifier: ISC
  ---------------------------------------------------------------------------*)

open B0_testing
open Result.Syntax
open Affect

let unix_sleepf = Unix.sleepf (* in case we redefine it in the future *)
let unix_read = Unix.read (* This read does not wait for data. *)
open Affect_unix

let test_ok ret = Test.(result ~ok:T.unit) (Ok ()) ret

let domain_count = Test_common.domain_count

(* Mtime and Ptime *)

let trace_mtime_wait ~exp ~actual =
  let pp = Mtime.Span.pp in
  Fun.Async.trace "Waited for %a +%a" pp exp pp (Mtime.Span.abs_sub actual exp)

let trace_ptime_wait ~exp ~actual =
  let diff = Ptime.Span.to_mtime_span (Ptime.diff actual exp) |> Option.get in
  Fun.Async.trace "Waited until %a +%a" Ptime.pp exp Mtime.Span.pp diff

let test_passing_time =
  Test.test' domain_count "{Mtime,Ptime}.wait_*" @@ fun domain_count ->
  Unix.main ?domain_count @@ fun () ->
  begin ignore @@ Fun.Async.call @@ fun () ->
    let dur = Ptime.Span.of_mtime_span Mtime.Span.(10 * ms) in
    let now = Ptime.now () in
    let later = Ptime.add_span now dur |> Option.get in
    let actual = Ptime.observe_wait_until later in
    trace_ptime_wait ~exp:later ~actual
  end;
  let dur = Mtime.Span.(1 * ms) in
  let actual = Mtime.observe_wait_for dur in
  trace_mtime_wait ~exp:dur ~actual;
  let dur = Mtime.Span.(1 * us) in
  let actual = Mtime.observe_wait_for dur in
  trace_mtime_wait ~exp:dur ~actual;
  let dur = Mtime.Span.(1 * ns) in
  let actual = Mtime.observe_wait_for dur in
  trace_mtime_wait ~exp:dur ~actual;
  ()

(* Signals *)

let self_signal signal =
  Fun.Async.trace "sending %a" Unix.Signal.pp signal;
  Unix.kill (Unix.getpid ()) signal

let rec try_self_signal ~signal ~until =
  let dur = Mtime.Span.(1 * ms) in
  let wait_one_ms = Mtime.wait_for' dur `Timeout in
  let returned = Action.map (fun v -> `Returned v) until in
  let race = Action.choose [returned; wait_one_ms] in
  self_signal signal;
  let c = Mtime.counter () in
  match Action.invoke race with
  | `Returned v -> v
  | `Timeout ->
      let actual = Mtime.count c in
      trace_mtime_wait ~exp:dur ~actual:actual;
      try_self_signal ~signal ~until

let test_signal_wait =
  Test.test' domain_count "Unix.Signal.wait" @@ fun domain_count ->
  Unix.main ?domain_count @@ fun () ->
  Unix.Signal.set Sys.sigusr1 Waiters;
  let w = Fun.Async.call @@ fun () ->
    Unix.Signal.wait Sys.sigusr1; Sys.sigusr1;
  in
  (* Technically we could try to loop on Action.invoke_poll Fun.Async.get'
     with a relax, but let's bet on [Action.invoke_poll] being removed. *)
  Fun.Async.yield ();
  let v = try_self_signal ~signal:Sys.sigusr1 ~until:(Fun.Async.get' w) in
  Test.int Sys.sigusr1 v;
  ()

let test_signal_final =
  Test.test' domain_count "Unblocker block on signal" @@
  fun domain_count ->
  Unix.Signal.set Sys.sigusr1 Waiters;
  let retry = Atomic.make true in
  (* We check that the unblocker blocks if the only thing to unblock that
     remains is signals. We simulate an external signal delivery with this
     thread. *)
  let t = Thread.create (fun () ->
      while (Atomic.get retry) do
        self_signal Sys.sigusr1;
        if Atomic.get retry then unix_sleepf 0.001;
      done) ()
  in
  begin Unix.main ?domain_count @@ fun () ->
    Unix.Signal.wait Sys.sigusr1;
    Atomic.set retry false;
  end;
  Thread.join t;
  ()

let test_fd_ebadf =
  Test.test' domain_count "Unix.wait_{readable,writable} EBADF" @@
  fun domain_count -> Unix.main ?domain_count @@ fun () ->
  let fd = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Unix.close_noerr fd;
  Snap.raise (fun () -> Action.invoke (Unix.wait_readable fd ())) @> __POS_OF__
    (Unix.Unix_error (Unix.EBADF, "Unix.wait_readable", ""));
  Snap.raise (fun () -> Action.invoke (Unix.wait_writable fd ())) @> __POS_OF__
    (Unix.Unix_error (Unix.EBADF, "Unix.wait_writable", ""));
  ()

let test_fd_no_ebadf =
  Test.test' domain_count "Unix.wait_{readable,writable} no EBADF" @@
  fun domain_count -> Unix.main ?domain_count @@ fun () ->
  let fd = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  let wait_fd = Fun.Async.call @@ fun () ->
    let finally () = Unix.close_noerr fd in
    Fun.Async.protect ~finally @@ fun () ->
    Action.invoke (Unix.wait_readable fd ())
  in
  Fun.Async.cancel wait_fd;
  (* Here we may still have the blocker for [wait_readable] in the fd
     unblocker data but we should not raise EBADF. Let's invoke the
     unblocker once to make sure we don't EBADF. *)
  Mtime.wait_for Mtime.Span.(100 * us);
  ()

let test_fd_no_spurious_readable =
  Test.test' domain_count "Unix.wait_readable no spurious wakeup" @@
  fun domain_count -> Unix.main ?domain_count @@ fun () ->
  (* Pairs of fibers send one byte to each other through two pipes. Only
     one fiber reads from each pipe, and only one byte is in a pipe at a
     time. Thus, if a fiber finds no data after a wakeup, the wakeup is
     spurious. With more than one domain, a select must wake only the wait
     registrations that were in its fd sets. *)
  let pairs = 4 and rounds = 5_000 in
  let spurious = Atomic.make 0 in
  let pipe () =
    let r, w = Unix.pipe ~cloexec:true () in
    Unix.set_nonblock r; Unix.set_nonblock w; r, w
  in
  let rec read_byte fd buf =
    Action.invoke (Unix.wait_readable fd ());
    match unix_read fd buf 0 1 with
    | 1 -> ()
    | _ -> Test.failstop "End of file before the byte"
    | exception Unix.Unix_error ((EAGAIN | EWOULDBLOCK), _, _) ->
        Atomic.incr spurious; read_byte fd buf
  in
  let player ~first ~input ~output () =
    let buf = Bytes.make 1 'x' in
    if first then ignore (Unix.write output buf 0 1);
    for _ = 1 to rounds do
      read_byte input buf; ignore (Unix.write output buf 0 1)
    done
  in
  let pair () =
    let a_in, b_out = pipe () and b_in, a_out = pipe () in
    let a = Fun.Async.call (player ~first:true ~input:a_in ~output:a_out) in
    let b = Fun.Async.call (player ~first:false ~input:b_in ~output:b_out) in
    [a; b], [a_in; a_out; b_in; b_out]
  in
  let calls, fds = List.split (List.init pairs (fun _ -> pair ())) in
  List.iter Fun.Async.get (List.concat calls);
  List.iter Unix.close_noerr (List.concat fds);
  Test.int ~__POS__ (Atomic.get spurious) 0;
  ()

let test_ping_pong_server =
  Test.test' domain_count "Ping-pong server" @@ fun domain_count ->
  Test.error_to_fail @@ Unix.main ?domain_count @@ fun () ->
  let client ~peer = Fun.Async.call @@ fun () -> test_ok @@ Result.join @@
    Net.with_connection ~peer @@ fun c ->
    let* () = Net.Msg.send c "ping" in
    let* msg = Net.Msg.recv c in
    Test.(option T.string) msg (Some "pong") ~__POS__;
    Ok ()
  in
  let server ~on:endpoint =
    let handle_client c = Fun.Async.call_trap_exn @@ fun () -> test_ok @@
      Net.with_connection_close c @@ fun c ->
      let* msg = Net.Msg.recv c in
      let* () = Net.Msg.send c "pong" in
      Test.(option T.string) msg (Some "ping") ~__POS__;
      Ok ()
    in
    let* l = Net.Listener.open' ~endpoint () in
    let chosen = Net.Listener.endpoint l in
    let run =
      Fun.Async.call @@ fun () ->
      let finally () = Net.Listener.close_noerr l in
      Fun.Async.protect ~finally @@ fun () ->
      let rec loop l = match Net.Listener.accept l with
      | Error _ as e -> e
      | Ok c -> handle_client c; loop l
      in
      test_ok @@ loop l
    in
    Ok (chosen, run)
  in
  let* peer, server = server ~on:(`Host ("localhost", 0)) in
  let clients = List.init 10 (fun _i -> client ~peer) in
  ignore @@ Fun.Async.get_all clients;
  Fun.Async.cancel server;
  Ok ()

let test_net_msg_actions =
  Test.test' domain_count "Net.Msg actions" @@ fun domain_count ->
  Test.error_to_fail @@ Unix.main ?domain_count @@ fun () ->
  Net.with_listener ~on:(`Host ("localhost", 0)) @@ fun l ->
  let timed_accept l =
    let timeout = Mtime.wait_for' Mtime.Span.(2 * ms) `Timeout in
    let connection = Net.Listener.wait_connection l `Connection in
    Action.choose [timeout; connection]
  in
  let timed_recv c =
    let timeout = Mtime.wait_for' Mtime.Span.(2 * ms) `Timeout in
    let recv = Net.Msg.wait_recvable c `Recv in
    Action.choose [timeout; recv]
  in
  let proceed = Port.make () in
  test_ok @@ match Action.invoke (timed_accept l) with
  | `Connection -> Error "No connection possible"
  | `Timeout ->
      let peer = Net.Listener.endpoint l in
      let _client =
        Fun.Async.call_trap_exn @@ fun () ->
        test_ok @@ Result.join @@
        Net.with_connection ~peer @@ fun c ->
        Port.offer proceed ();
        match Action.invoke (timed_recv c) with
        | `Recv -> Error "No receive possible"
        | `Timeout ->
            Port.take proceed;
            Port.take proceed;
            let () = match Action.invoke (timed_recv c) with
            | `Timeout -> () (* Still happens sometimes, timeout is short *)
            | `Recv -> ()
            in
            let* msg = Net.Msg.recv c in
            Test.(option T.string) msg (Some "ping");
            Ok ()
      in
      Port.take proceed;
      begin match Action.invoke (timed_accept l) with
      | `Timeout -> Error "Should not happen, peer connected"
      | `Connection ->
          let* c = Net.Listener.accept l in
          Net.with_connection_close c @@ fun c ->
          Port.offer proceed ();
          let* () = Net.Msg.send c "ping" in
          Port.offer proceed ();
          Ok ()
      end

let () = if !Sys.interactive then () else exit (Test_common.main ())
