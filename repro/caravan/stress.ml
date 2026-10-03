(* Many rounds of: a server, 20 concurrent clients doing one GET with
   Connection: close each. Reports the first round where a client waits
   more than 3s, with what each side got to. *)
open Affect
open Caravan
module Mtime = Affect_unix.Mtime

let () =
  let rounds = int_of_string Sys.argv.(1) and domains = int_of_string Sys.argv.(2) in
  for round = 1 to rounds do
    let accepted = Atomic.make 0 and served = Atomic.make 0 and finished = Atomic.make 0 in
    let stage = Array.make 20 "start" in
    (* MINI=1: a minimal engine, read a request, run a call, then wait on
       [choose [get' call; readable]] and answer. *)
    let mini (_ : Engine.ctx) flow (_ : Handler.t) =
      (* REUSE=1: wait on the flow's readability action before reading, as
         the engine does, so that the choice below invokes it a second time. *)
      if Sys.getenv_opt "REUSE" = Some "1" then Action.invoke (Flow.readable' flow ());
      ignore (Flow.read flow (Bytes.create 1024) 0 1024);
      let call = Fun.Async.call (fun () -> Atomic.incr served; "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello") in
      let reply =
        match
          Action.invoke
            (Action.choose [ Action.map (fun r -> `Reply r) (Fun.Async.get' call); Flow.readable' flow `Readable ])
        with
        | `Reply r -> r
        | `Readable -> Fun.Async.get call
      in
      Flow.write_string flow reply
    in
    (* mini2: the real request path around the same choice. *)
    let mini2 (ctx : Engine.ctx) flow (h : Handler.t) =
      let open Private in
      let r = Buf_reader.make ~size:4096 (fun buf off len -> Flow.read flow buf off len) in
      let w = Buf_writer.make ~size:4096 (Flow.write flow) in
      let head = H1_parse.request_head (H1_server.limits ctx.config) r in
      let req = Request.make ~headers:head.headers head.meth head.target in
      let call = Fun.Async.call (fun () -> h req) in
      let response =
        match
          Action.invoke
            (Action.choose [ Action.map (fun r -> `Reply r) (Fun.Async.get' call); Flow.readable' flow `Readable ])
        with
        | `Reply r -> r
        | `Readable -> Fun.Async.get call
      in
      let framing = H1_write.framing ~meth:head.meth ~version:head.version response in
      H1_write.head w ~version:head.version ~keep_alive:false framing response;
      match Response.kind response with
      | Response.Body b -> H1_write.body w framing b
      | Response.Upgrade _ -> ()
    in
    (* mini3: mini2 plus the old engine's surroundings, each switchable:
       M3_IDLE the idle choice, M3_DEADLINE read deadlines, M3_CELL the
       body's completion choice, M3_LAZY the lazy head write. *)
    let on name = Sys.getenv_opt name <> Some "0" in
    let mini3 (ctx : Engine.ctx) flow (h : Handler.t) =
      let open Private in
      let deadline = ref None in
      let r =
        Buf_reader.make ~size:4096 (fun buf off len ->
            if on "M3_DEADLINE" then Flow.set_read_deadline flow !deadline;
            Flow.read flow buf off len)
      in
      let w = Buf_writer.make ~size:4096 (Flow.write flow) in
      if on "M3_IDLE" then begin
        let stop = Action.map (fun () -> `Stop) ctx.stopping in
        match
          Action.invoke
            (Action.choose [ stop; Flow.readable' flow `Input; Mtime.wait_for' Mtime.Span.(30 * s) `Idle ])
        with
        | `Stop | `Idle -> ()
        | `Input -> ignore (Buf_reader.peek_eof r)
      end;
      deadline := Mtime.add_span (Mtime.now ()) Mtime.Span.(10 * s);
      let head = H1_parse.request_head (H1_server.limits ctx.config) r in
      let body = H1_body.make ~limits:(H1_server.limits ctx.config) r No_body in
      let req = Request.make ~headers:head.headers ~body:(H1_body.body body) head.meth head.target in
      let call = Fun.Async.call (fun () -> h req) in
      let get = Action.map (fun r -> `Reply r) (Fun.Async.get' call) in
      let response =
        match
          if on "M3_CELL"
          then Action.invoke (Action.choose [ get; Action.map (fun () -> `Read) (H1_body.completed body) ])
          else `Read
        with
        | `Reply r -> r
        | `Read -> (
            match Action.invoke (Action.choose [ get; Flow.readable' flow `Readable ]) with
            | `Reply r -> r
            | `Readable -> Fun.Async.get call)
      in
      let framing = H1_write.framing ~meth:head.meth ~version:head.version response in
      let write_head () = H1_write.head w ~version:head.version ~keep_alive:false framing response in
      match Response.kind response with
      | Response.Body b ->
          if on "M3_LAZY" then H1_write.body ~before:write_head w framing b
          else (write_head (); H1_write.body w framing b)
      | Response.Upgrade _ -> ()
    in
    let engine ctx flow h =
      Atomic.incr accepted;
      Fun.protect ~finally:(fun () -> Atomic.incr finished) (fun () ->
          match Sys.getenv_opt "ENGINE" with
          | Some "mini" -> mini ctx flow h
          | Some "mini2" -> mini2 ctx flow h
          | Some "mini3" -> mini3 ctx flow h
          | Some "old" -> Old_engine.serve ctx flow h
          | _ -> Private.H1_server.serve ctx flow h)
    in
    let handler _ = Atomic.incr served; Response.string "hello" in
    let hung = ref false in
    Affect_unix.Unix.main ~domain_count:domains (fun () ->
        let config = { Config.default with addr = Unix.ADDR_INET (Unix.inet_addr_loopback, 0); date_header = false; cancel_on_disconnect = (Sys.getenv_opt "WATCH" <> Some "0") } in
        let server = Server.start ~config ~engine handler in
        let client i () =
          stage.(i) <- "connecting";
          let fd = Affect_unix.Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
          Affect_unix.Unix.connect fd (Server.local_addr server);
          let port = match Unix.getsockname fd with Unix.ADDR_INET (_, p) -> p | _ -> 0 in
          let c = Tcp.flow fd in
          Flow.set_read_deadline c (Mtime.add_span (Mtime.now ()) Mtime.Span.(3 * s));
          stage.(i) <- "writing";
          Flow.write_string c "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
          stage.(i) <- "reading";
          let buf = Bytes.create 1024 in
          let rec loop got = match Flow.read c buf 0 1024 with 0 -> got | n -> loop (got + n) in
          (match loop 0 with
          | n -> stage.(i) <- Printf.sprintf "done %d" n
          | exception Flow.Timeout ->
              stage.(i) <- Printf.sprintf "TIMEOUT reading (fd %d, port %d)" (Obj.magic fd : int) port;
              ignore (Sys.command (Printf.sprintf "ss -tnoi '( sport = :%d or dport = :%d )' 1>&2; ls -l /proc/%d/fd | grep -c socket 1>&2" port port (Unix.getpid ())));
              hung := true);
          Flow.close c
        in
        let cs = List.init 20 (fun i -> Fun.Async.call (client i)) in
        List.iter (fun c -> try Fun.Async.get c with e -> hung := true; Printf.printf "client raised %s\n%!" (Printexc.to_string e)) cs;
        if !hung then
          Printf.printf "round %d: accepted=%d served=%d finished=%d\n  %s\n%!" round (Atomic.get accepted)
            (Atomic.get served) (Atomic.get finished)
            (String.concat "; " (Array.to_list (Array.mapi (fun i s -> Printf.sprintf "%d:%s" i s) stage)));
        if !hung then Printf.printf "  server stages not done: %s\n%!" (Old_engine.dump ());
        if !hung || Sys.getenv_opt "DBG_ALWAYS" = Some "1" then Option.iter Affect__base.Dbg.dump (Sys.getenv_opt "DBG_OUT");

        Server.shutdown server; Server.wait server);
    if !hung then exit 1
  done;
  print_endline "no hang"
