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
      email: "",
      preferred_username: "",
      family_name: "",
      given_name: "",
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
      user.email = "";
      user.preferred_username = "";
      user.family_name = "";
      user.given_name = "";
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
