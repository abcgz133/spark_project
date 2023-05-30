package sparkProject.the4th_other_projects.frameWork_Best10.application

import exercise.frameWork_Best10.comm.Trait_Best10_Application
import exercise.frameWork_Best10.controller.Best10_Controller

object Best10_Application extends  App with Trait_Best10_Application{

  start(){
    val best10_controller = new Best10_Controller()
    best10_controller.dispatch()
  }

}
