import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { AckType } from "../internal/pubsub/subscribeJSON.js";
export function handlerPause(gs) {
    return (ps) => {
        handlePause(gs, ps);
        process.stdout.write("> ");
        return AckType.Ack;
    };
}
export function handlerMove(gs) {
    return (move) => {
        const outcome = handleMove(gs, move);
        process.stdout.write("> ");
        if (outcome === MoveOutcome.Safe || outcome === MoveOutcome.MakeWar) {
            return AckType.Ack;
        }
        return AckType.NackDiscard;
    };
}
