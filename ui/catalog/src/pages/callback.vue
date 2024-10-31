<template>
  <div>Callback</div>
</template>
<script setup lang="ts">
import { UserManager } from "oidc-client-ts";
import { useUserStore } from "../stores/user";
import { User } from "@/common/interfaces";
// import * as env from "../app.config";
import router from "@/router";
import { useAuth } from "../plugins/auth";

const settings = useAuth().oidcSettings;

const userStorage = useUserStore();

const userManager = new UserManager(settings);

(async () => {
  try {
    console.log("callback");
    const user = await userManager.signinRedirectCallback();
    const token = user.access_token;

    const newUser: User = {
      access_token: user.access_token,
      id_token: user.id_token || "",
      refresh_token: user.refresh_token || "",
      token_expires_at: user.profile.exp,
      email: user.profile.email || "",
      preferred_username: user.profile.preferred_username || "",
      family_name: user.profile.family_name || "",
      given_name: user.profile.given_name || "",
    };

    userStorage.setUser(newUser);

    const fetchUsers = async () => {
      try {
        const response = await fetch(
          "http://localhost:8081/management/v1/info",
          {
            method: "GET",
            headers: {
              Authorization: `Bearer ${token}`,
              "Content-Type": "application/json",
            },
          }
        );
        if (!response.ok) {
          throw new Error(`Error: ${response.statusText}`);
        }
        const data = await response.json();

        if (!data.bootstrapped) {
          router.push("/bootstrap");
        } else {
          router.push("/");
        }
      } catch (err) {
        console.error(err);
      }
    };
    fetchUsers();
  } catch (error) {
    console.error("Error during callback processing:", error);
  }
})();
</script>
