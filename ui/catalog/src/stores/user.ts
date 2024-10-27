// Utilities
import { User } from "@/common/interfaces";
import { defineStore } from "pinia";

export const useUserStore = defineStore(
  "user",
  () => {
    const isAuthenticated = ref(false);

    const user: User = reactive({
      access_token: "",
      id_token: "",
      email: "anonymous@unknown.com",
      preferred_username: "anonymous",
      family_name: "Ymous",
      given_name: "Anon",
    });

    function setUser(newUser: User) {
      isAuthenticated.value = true;
      Object.assign(user, newUser);
    }

    function getUser(): User {
      return user;
    }

    function unsetUser() {
      isAuthenticated.value = false;
      user.access_token = "";
      user.id_token = "";
      user.email = "anonymous@unknown.com";
      user.preferred_username = "anonymous";
      user.family_name = "Ymous";
      user.given_name = "Anon";
    }

    return { isAuthenticated, user, unsetUser, setUser, getUser };
  },
  {
    persistedState: {
      key: "user",
      persist: true,
    },
  }
);
