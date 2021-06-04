defmodule Paxos do
    #Register each of the processes from the upper layer
    def start(name, participants,upper_layer) do
        pid = spawn(Paxos, :init, [name, participants,upper_layer])
        case :global.re_register_name(name, pid) do
            :yes -> pid  
            :no  -> :error
        end
        IO.puts "registered #{name}"
        pid

    end
    #initalisation function to set the default state for each of the processes.
    def init(name, participants,upper_layer) do 
        state = %{
            name: name,
            participants: participants,
            upper_layer: upper_layer,
            ballot: 0,
            accepted_ballot: 0,
            value: nil,
            accepted_value: nil,
            trusted_process: nil,
            chosen: false,
            prepare_broadcast: false,
            locked: false,
            prepared_procs: %{},
            accepted_procs: %{},
            prepared_counter: 0,
            round: 1,


        }
        run(state)

    end
    #Run function, which handles all messages received by the process.
    def run(state) do
        state = receive do
            #Upon a new proposal
            {:propose, value} ->
                state = %{state | value: value}
                check_internal_events(state)
                state
            #Upon a trust even from the Leader Detector
            {:trust, p} ->
                state = %{state | trusted_process: p}
                check_internal_events(state)
                state
            #Upon a decide message being sent from a leader process, v is the value decided by said leader
            {:decide, v} ->
                state = if state.chosen != true do
                    state = %{state | chosen: true}
                    send(state.upper_layer, {:decide, v})
                    state
                else
                    state
                end
                state
            #Upon receiving a prepare message from the leader
            {:prepare, p, b} ->
                state = if b > state.ballot do
                    state = %{state | ballot: b}
                    send(p, {:prepared, state.ballot, state.accepted_ballot, state.accepted_value})
                    state 
                else
                    send(p, {:nack, b})
                    state
                end
                state
            #Upon receiving an accept message from a leader process
            {:accept, p, b, v} ->
                state = if b >= state.ballot do
                    state = %{state | ballot: b, accepted_ballot: b, accepted_value: v}
                    send(p, {:accepted, self(), state.accepted_ballot})
                    state
                else
                    send(p, {:nack, b})
                    state
                end
                state
            #Upon leader receiving a prepared message from another process thorugh pl
            {:prepared, b, a_bal, a_val} ->
                state = if b == state.ballot and state.locked != true do
                    %{state | prepared_procs: Map.put(state.prepared_procs, a_bal, a_val), prepared_counter: state.prepared_counter+1}
                else
                    state
                end
                state = check_internal_events(state)
                state
            #Upon leader receiving an accepted message from another process thorugh pl
            {:accepted, p, b} -> 
                state = if b == state.ballot do
                    %{state | accepted_procs: Map.put(state.accepted_procs, p, b)}
                else
                    state
                end
                state = check_internal_events(state)
                state
            #Upon receiving a negative acknowledgement from a previously sent prepare or accept message
            {:nack, b} ->
                state = if b >= state.ballot do 
                    %{state | locked: false, prepared_procs: %{}, accepted_procs: %{}, prepare_broadcast: false, prepared_counter: 0}
                else
                    state
                end
                check_internal_events(state)
        end
        run(state)
    end

    #Function to generate increasing unique ballot numbers for each of the leader processes
    def generateNewBallot(state) do
        state = %{state | round: state.round + 1}
        newBallot = state.round * length(state.participants) + Enum.find_index(state.participants, fn(x) -> (x == state.name) end)
        state = if newBallot > state.ballot do
            %{state | ballot: newBallot}
        else
            generateNewBallot(state)
        end
        state
    end
        
# Send message m point-to-point to process p
    defp unicast(m, p) do
        case :global.whereis_name(p) do
                pid when is_pid(pid) and pid != self() -> send(pid, m)
                pid when pid == self() -> :ok
                :undefined -> :ok
        end
    end

    # Best-effort broadcast of m to the set of destinations dest
    defp beb_broadcast(m, dest), do: for p <- dest, do: unicast(m, p)

    def check_internal_events(state) do
        #Upon process becoming a leader, generate a new ballot and send out the prepare message.
        state = if state.name == state.trusted_process and state.prepare_broadcast == false and state.value != nil and state.chosen == false and state.locked == false do
            state = %{state | prepare_broadcast: true, prepared_counter: 1}
            state = generateNewBallot(state)
            beb_broadcast({:prepare, self(), state.ballot}, state.participants)
            %{state | prepared_procs: Map.put(state.prepared_procs, state.ballot, state.value)}
        else
            state
        end

        #Upon a quorum of prepared messages received, lock this phase and continue onto accept, but first adopting the value of the highest accepted ballot received, unless the max accepted ballot = ballot.
        state = if state.name == state.trusted_process and state.prepared_counter > trunc(length(state.participants)/2)and state.locked == false and state.chosen == false do
            state = if Enum.max(Map.keys(state.prepared_procs)) != state.ballot do
                %{state | locked: true, value: Map.get(state.prepared_procs, Enum.max(Map.keys(state.prepared_procs)))}
            else
                %{state | locked: true}
            end
            beb_broadcast({:accept, self(), state.ballot, state.value}, state.participants)
            %{state | accepted_procs: Map.put(state.accepted_procs, state.name, state.ballot)}
        else
            state
        end
        #Upon receiving a quorum of accepted messages, send a trigger decide on self and broadcast a decide message to all processing, causing them to decide on the same value
        state = if state.name == state.trusted_process and length(Map.keys(state.accepted_procs)) > trunc(length(state.participants)/2) and state.chosen == false do
            send(self(), {:decide, state.value})
            beb_broadcast({:decide, state.value}, state.participants)
            state
        else
            state
        end
    end

end