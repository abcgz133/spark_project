package creditcard_sparkProject.frameWork_Best10.controller

import exercise.frameWork_Best10.comm.Trait_Best10_Controller
import exercise.frameWork_Best10.service.Best10_Service

class Best10_Controller extends  Trait_Best10_Controller{
  private val best10_Service = new Best10_Service()
  override def dispatch(): Unit = {
    val tuplesArray: Array[(String, (Int, Int, Int))] = best10_Service.best10_analysis()
    tuplesArray.foreach(x => println(s"category_id: ${x._1} clicking number:${x._2._1} booking number:${x._2._2} paying number:${x._2._3}"))

  }
}
