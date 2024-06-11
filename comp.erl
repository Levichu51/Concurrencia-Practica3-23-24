-module(comp).

-export([comp/1, comp_proc/2, comp_proc/3, decomp/1, decomp/2, decomp_proc/2, decomp_proc/3]).
-export([comp_worker_loop/3, decomp_worker_loop/3]).

-define(DEFAULT_CHUNK_SIZE, 1024*1024).
-define(DEFAULT_PROCS, 3).

%%% File Compression (Ej1)

comp(File) ->
    comp_proc(File, ?DEFAULT_CHUNK_SIZE, ?DEFAULT_PROCS).

comp_proc(File, Procs) ->
    comp_proc(File, ?DEFAULT_CHUNK_SIZE, Procs).

comp_proc(File, Chunk_Size, Procs) ->
    case start_processes(File, Chunk_Size, Procs) of
        {ok, Reader, Writer, Workers} ->
            aux_loop(Workers, Reader, Writer);
        {error, Reason} ->
            io:format("Error: ~s~n", [Reason])
    end.

start_processes(File, Chunk_Size, Procs) ->
    case file_service:start_file_reader(File, Chunk_Size) of
        {ok, Reader} ->
            case archive:start_archive_writer(File ++ ".ch") of
                {ok, Writer} ->
                    Workers = start_workers(Procs, Reader, Writer, []),
                    {ok, Reader, Writer, Workers};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

start_workers(0, _, _, Workers) -> Workers;
start_workers(Procs, Reader, Writer, Workers) ->
    NewPid = spawn_link(?MODULE,  comp_worker_loop, [Reader, Writer, self()]),
    start_workers(Procs - 1, Reader, Writer, [NewPid | Workers]).

comp_worker_loop(Reader, Writer, Shell) ->  
    Reader ! {get_chunk, self()},
    receive
        {chunk, Num, Offset, Data} ->   
            Comp_Data = compress:compress(Data),
            Writer ! {add_chunk, Num, Offset, Comp_Data},
            comp_worker_loop(Reader, Writer, Shell);
        eof ->  
            Shell ! {done, self()},
            ok;
        {error, Reason} ->
            Shell ! {error, Reason},
            ok;
        stop -> ok
    end.

aux_loop([], Reader, Writer) ->
    Reader ! stop,
    Writer ! stop;
aux_loop(Workers, Reader, Writer) ->
    receive
        {done, Worker} ->
            WorkersLeft = lists:delete(Worker, Workers),
            aux_loop(WorkersLeft, Reader, Writer);
        {error, Reason} ->
            io:format("Error: ~s~n", [Reason]),
            stop_processes(Workers, Reader, Writer)
    end.

stop_processes([], _, _) -> ok;
stop_processes([Worker | Workers], Reader, Writer) ->
    Worker ! stop,
    stop_processes(Workers, Reader, Writer).




%% File Decompression (Ej2)

decomp(Archive) ->
    decomp(Archive, string:replace(Archive, ".ch", "", trailing)).

decomp(Archive, Output_File) ->
    case archive:start_archive_reader(Archive) of
        {ok, Reader} ->
            case file_service:start_file_writer(Output_File) of
                {ok, Writer} ->
                    decomp_loop(Reader, Writer);
                {error, Reason} ->
                    io:format("Could not open output file: ~w~n", [Reason])
            end;
        {error, Reason} ->
            io:format(" Could not open input file: ~w~n", [Reason])
    end.

decomp_loop(Reader, Writer) ->
    Reader ! {get_chunk, self()},  %% request a chunk from the reader
    receive
        {chunk, _Num, Offset, Comp_Data} ->  %% got one
            Data = compress:decompress(Comp_Data),
            Writer ! {write_chunk, Offset, Data},
            decomp_loop(Reader, Writer);
        eof ->    %% end of file => exit decompression
            Reader ! stop,
            Writer ! stop;
        {error, Reason} ->
            io:format("Error reading input file: ~w~n", [Reason]),
            Writer ! abort,
            Reader ! stop
    end.



decomp_proc(Archive, Procs) ->
    Output_File = string:replace(Archive, ".ch", "", trailing),
    decomp_proc(Archive, Output_File, Procs).

decomp_proc(Archive, Output_File, Procs) ->
    case start_processes2(Archive, Output_File, Procs) of
        {ok, Reader, Writer, Workers} ->
            aux_loop2(Workers, Reader, Writer);
        {error, Reason} ->
            io:format("Error: ~s~n", [Reason])
    end.

start_processes2(Archive, Output_File, Procs) ->
    case archive:start_archive_reader(Archive) of
        {ok, Reader} ->
            case file_service:start_file_writer(Output_File) of
                {ok, Writer} ->
                    Workers = start_workers2(Procs, Reader, Writer, []),
                    {ok, Reader, Writer, Workers};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

start_workers2(0, _, _, Workers) -> Workers;
start_workers2(Procs, Reader, Writer, Workers) ->
    NewPid = spawn_link(?MODULE,  decomp_worker_loop, [Reader, Writer, self()]),    
    start_workers2(Procs - 1, Reader, Writer, [NewPid | Workers]).


decomp_worker_loop(Reader, Writer, Shell) ->
    Reader ! {get_chunk, self()},
    receive
        {chunk, _Num, Offset, Comp_Data} ->
            Data = compress:decompress(Comp_Data),
            Writer ! {write_chunk, Offset, Data},
            decomp_worker_loop(Reader, Writer, Shell);
        eof ->
            Shell ! {done, self()},
            ok;
        {error, Reason} ->
            Shell ! {error, Reason},
            ok;
        stop -> ok
    end.

aux_loop2([], Reader, Writer) ->
    Reader ! stop,
    Writer ! stop;
aux_loop2(Workers, Reader, Writer) ->
    receive
        {done, Worker} ->
            WorkersLeft = lists:delete(Worker, Workers),
            aux_loop2(WorkersLeft, Reader, Writer);
        {error, Reason} ->
            io:format("Error: ~s~n", [Reason]),
            stop_processes2(Workers, Reader, Writer)
    end.

stop_processes2([], _, _) -> ok;
stop_processes2([Worker | Workers], Reader, Writer) ->
    Worker ! stop,
    stop_processes2(Workers, Reader, Writer).





