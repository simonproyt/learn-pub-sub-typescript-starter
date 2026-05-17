import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import type { ArmyMove, RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import type { ConfirmChannel } from "amqplib";
import { AckType } from "../internal/pubsub/subscribeJSON.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilTopic, WarRecognitionsPrefix } from "../internal/routing/routing.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  return (ps: PlayingState) => {
    handlePause(gs, ps);
    process.stdout.write("> ");
    return AckType.Ack;
  };
}

export function handlerMove(
  gs: GameState,
  publishChannel: ConfirmChannel,
): (move: ArmyMove) => Promise<AckType> {
  return async (move: ArmyMove) => {
    const outcome = handleMove(gs, move);
    if (outcome === MoveOutcome.MakeWar) {
      const rw: RecognitionOfWar = {
        attacker: move.player,
        defender: gs.getPlayerSnap(),
      };
      const routingKey = `${WarRecognitionsPrefix}.${gs.getPlayerSnap().username}`;
      try {
        await publishJSON(publishChannel, ExchangePerilTopic, routingKey, rw);
        console.log(`Published war recognition to ${routingKey}`);
        process.stdout.write("> ");
        return AckType.Ack;
      } catch (err) {
        console.error("Failed to publish war recognition, requeueing move:", err);
        process.stdout.write("> ");
        return AckType.NackRequeue;
      }
    }

    process.stdout.write("> ");
    if (outcome === MoveOutcome.Safe) {
      return AckType.Ack;
    }
    return AckType.NackDiscard;
  };
}

export function handlerWar(
  gs: GameState,
  publishGameLog: (username: string, message: string) => Promise<void>,
): (rw: RecognitionOfWar) => Promise<AckType> {
  return async (rw: RecognitionOfWar) => {
    const result = handleWar(gs, rw);
    let logMessage: string | null = null;

    switch (result.result) {
      case WarOutcome.NotInvolved:
        process.stdout.write("> ");
        return AckType.NackRequeue;
      case WarOutcome.NoUnits:
        process.stdout.write("> ");
        return AckType.NackDiscard;
      case WarOutcome.OpponentWon:
      case WarOutcome.YouWon:
        logMessage = `${result.winner} won a war against ${result.loser}`;
        break;
      case WarOutcome.Draw:
        logMessage = `A war between ${result.attacker} and ${result.defender} resulted in a draw`;
        break;
      default:
        console.error("Unknown war outcome:", result);
        process.stdout.write("> ");
        return AckType.NackDiscard;
    }

    try {
      await publishGameLog(rw.attacker.username, logMessage);
      process.stdout.write("> ");
      return AckType.Ack;
    } catch (err) {
      console.error("Failed to publish war log, requeueing:", err);
      process.stdout.write("> ");
      return AckType.NackRequeue;
    }
  };
}
