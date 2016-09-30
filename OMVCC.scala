import java.util._

// IMPORTANT -- THIS IS INDIVIDUAL WORK. ABSOLUTELY NO COLLABORATION!!!


// - implement a (main-memory) data store with OMVCC.
// - objects are <int, int> key-value pairs.
// - if an operation is to be refused by the OMVCC protocol,
//   undo its xact (what work does this take?) and throw an exception.
// - garbage collection of versions is optional.
// - throw exceptions when necessary, such as when we try to:
//   + execute an operation in a transaction that is not running
//   + read a nonexisting key
//   + delete a nonexisting key
//   + write into a key where it already has an uncommitted version
// - you may but do not need to create different exceptions for operations that
//   are refused and for operations that are refused and cause the Xact to be
//   aborted. Keep it simple!
// - keep the interface, we want to test automatically!

private class Version(val k: Int, val v: Int, val ts: Long) {
  val key: Int = k
  var value: Int = v
  var timeStamp: Long = ts
  
  def commited: Boolean = {
    timeStamp < (1L << 62)
  }

  override def equals(v2: Any): Boolean = v2 match{
    case v2: Version => this.value == v2.value && this.timeStamp == v2.timeStamp
    case _ => false
  }
}

private class Predicate(val t: Char, val v: Int) {
  var typ: Char = t
  var value: Int = v
  
  def matches(v: Version): Boolean = {
    if(typ == 'r') {
      if(v.key == value) true
      else false
    }
    else {
      if(v.value % value == 0) true
      else false
    }
  }
  
  override def equals(p2: Any): Boolean = p2 match{
    case p2: Predicate => this.typ == p2.typ && this.value == p2.value
    case _ => false
  }
}

private class Transaction(val sts: Long, val tid: Long) {
  var startTimeStamp: Long = sts
  var ID: Long = tid
  var commitTimeStamp: Long = 0
  var undoBuffer = collection.mutable.Set[Version]()
  var predicates = collection.mutable.Set[Predicate]()
  override def equals(t2: Any): Boolean = t2 match{
   case t2: Transaction => this.ID == t2.ID
   case t2: Long => t2 == this.ID
   case _ => false
  }
}

class TransactionException(message: String = null, cause: Throwable = null) extends RuntimeException(message, cause)
class KeyException(message: String = null, cause: Throwable = null) extends RuntimeException(message, cause)
class WriteException(message: String = null, cause: Throwable = null) extends RuntimeException(message, cause)
class CommitException(message: String = null, cause: Throwable = null) extends RuntimeException(message, cause)

object OMVCC {
  /* TODO -- your versioned key-value store data structure */
  private var keyValue = scala.collection.mutable.Map[Int, scala.collection.mutable.Set[Version]]()
  private var activeTransactions = scala.collection.mutable.Set[Transaction]()
  private var commitedTransactions = scala.collection.mutable.Set[Transaction]()
  private var startAndCommitTimestampGen: Long = 0
  private var transactionIdGen: Long = 1L << 62

  // returns transaction id == logical start timestamp
  def begin: Long = {
    startAndCommitTimestampGen += 1  //SHOULD BE USED
    transactionIdGen += 1  //SHOULD BE USED
    activeTransactions += new Transaction(startAndCommitTimestampGen, transactionIdGen)
    transactionIdGen
  }

  // return value of object key in transaction xact
  @throws(classOf[Exception])
  def read(xact: Long, key: Int): Int = {
    checkXact(xact, "read")
    if(!keyValue.keySet.contains(key)){
      rollback(xact)
      throw new KeyException("Trying to read a key that doesn't exist!")  
    }
    else {
      val trans = activeTransactions.find(_ == xact).get
      val vers = keyValue(key).filter(x => (x.timeStamp < trans.startTimeStamp && x.commited) || x.timeStamp == trans.ID)
      if(vers.size > 0) {
        activeTransactions.find(_ == xact).get.predicates += new Predicate('r', key)
        vers.maxBy(_.timeStamp).value
      }
      else {
        rollback(xact)
        throw new KeyException("Key not visible to transaction!")
      }
    }
  }

  // return the list of keys of objects whose values mod k are zero.
  // this is our only kind of query / bulk read.
  @throws(classOf[Exception])
  def modquery(xact: Long, k: Int): java.util.List[Integer] = {
    val l = new java.util.ArrayList[Integer]
    checkXact(xact, "do a mod query")
    activeTransactions.find(_ == xact).get.predicates += new Predicate('m', k)
    val trans = activeTransactions.find(_ == xact).get
    for((key, versions) <- keyValue) {
      val vers = versions.filter(x => (x.timeStamp < trans.startTimeStamp && x.commited) || x.timeStamp == trans.ID)
      if(vers.size > 0) {
        val value = vers.maxBy(_.timeStamp).value
        if(value % k == 0) l.add(value)
      }
    }
    l
  }

  // update the value of an existing object identified by key
  // or insert <key,value> for a non-existing key in transaction xact
  @throws(classOf[Exception])
  def write(xact: Long, key: Int, value: Int) {
    checkXact(xact, "write")
    val trans = activeTransactions.find(_ == xact).get
    if(keyValue.contains(key)) {
      val vers = keyValue(key)
      if(vers.filter( x => !x.commited && x.timeStamp != trans.ID).size != 0) {
        rollback(xact)
        throw new WriteException("Write impossible: existing uncommited version for this key!")
      }
      else if (vers.filter(x => x.commited && x.timeStamp > trans.startTimeStamp).size != 0) {
        rollback(xact)
        throw new WriteException("Write impossible: more recent commited version existing!")
      }
      else if (trans.undoBuffer.exists(_.key == key)){
        keyValue(key).find(_.timeStamp == trans.ID).get.value = value
        activeTransactions.find(_ == xact).get.undoBuffer.find(_.key == key).get.value = value
      }
      else {
        val ver = new Version(key, value, trans.ID)
        activeTransactions.find(_ == xact).get.undoBuffer += ver
        keyValue(key) += ver
      }
    }
    else {
      val ver = new Version(key, value, trans.ID)
      activeTransactions.find(_ == xact).get.undoBuffer += ver
      keyValue += (key -> scala.collection.mutable.Set[Version](ver))
    }
  }

  // delete the object identified by key in transaction xact
  @throws(classOf[Exception])
  def delete(xact: Long, key: Int) {
    /* TODO */
  }

  @throws(classOf[Exception])
  def commit(xact: Long) {
    checkXact(xact, "commit")
    val trans = activeTransactions.find(_ == xact).get
    if(trans.undoBuffer.size == 0) {
      trans.commitTimeStamp = trans.startTimeStamp
      activeTransactions.remove(trans)
      commitedTransactions.add(trans)
    }
    else{
      for(t2 <- commitedTransactions.filter(_.commitTimeStamp > trans.startTimeStamp)){
        for(v <- t2.undoBuffer){
          for(p <- trans.predicates) {
            if(p.matches(v)) {
              rollback(xact)
              throw new CommitException("Commit validation failed!")
            }
          }
        }
      }
      startAndCommitTimestampGen += 1 //SHOULD BE USED
      trans.commitTimeStamp = startAndCommitTimestampGen
      for((k,v) <- keyValue) {
        for(version <- v) {
          if(version.timeStamp == trans.ID && trans.undoBuffer.contains(version)){
            version.timeStamp = trans.commitTimeStamp
          }
        }
      }
      activeTransactions.remove(trans)
      commitedTransactions.add(trans)
    }
  }

  @throws(classOf[Exception])
  def rollback(xact: Long) {
    checkXact(xact, "rollback")
    val trans = activeTransactions.find(_ == xact).get
    for(v <- trans.undoBuffer) {
      keyValue(v.key).remove(v)
    }
    activeTransactions.remove(trans)
  }
  
  @throws(classOf[Exception])
  def checkXact(xact: Long, message: String) {
    if(activeTransactions.filter(_ == xact).size == 0) {
      if(commitedTransactions.filter(_ == xact).size == 0) {
        rollback(xact)
        throw new TransactionException("Trying to " + message + " from an unexisting transaction!")
      }
      else {
        rollback(xact)
        throw new TransactionException("Trying to " + message + " from a commited transaction!")
      }
    }
  }
}

