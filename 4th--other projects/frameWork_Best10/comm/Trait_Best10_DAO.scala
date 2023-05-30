package creditcard_sparkProject.frameWork_Best10.comm

import exercise.frameWork_Best10.util.BufferUtil

trait Trait_Best10_DAO {

  def ReadFile(path:String) ={

    BufferUtil.get().textFile(path)
  }
}
