package src

import com.yourorg.yourproject.proto.user.UserOuterClass

object MainApp {
  @JvmStatic
  fun main(args: Array<String>) {
    val user = UserOuterClass.User.newBuilder()
      .setEmail("a,coin")
      .build()
    println(user)
  }
}