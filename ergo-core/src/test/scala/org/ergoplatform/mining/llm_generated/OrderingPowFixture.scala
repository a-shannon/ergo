package org.ergoplatform.mining.llm_generated

import com.google.common.primitives.Longs
import org.ergoplatform.{AutolykosSolution, BlockSolutionSearchResult, NoSolutionFound, OrderingSolutionFound, SolutionFound}

import scala.annotation.tailrec

/** Test-only selection from the ordinary typed result returned by a bounded nonce search. */
private[mining] object OrderingPowFixture {
  def find(startNonce: Long,
           endNonce: Long,
           accept: AutolykosSolution => Boolean)
          (search: (Long, Long) => BlockSolutionSearchResult): Option[AutolykosSolution] = {
    // The search callback reports solutions inside its requested half-open interval.
    @tailrec
    def loop(cursor: Long): Option[AutolykosSolution] = {
      if (cursor >= endNonce) {
        None
      } else {
        search(cursor, endNonce) match {
          case OrderingSolutionFound(solution) if accept(solution) => Some(solution)
          case found: SolutionFound => loop(Longs.fromByteArray(found.as.n) + 1L)
          case NoSolutionFound => None
        }
      }
    }

    loop(startNonce)
  }
}
