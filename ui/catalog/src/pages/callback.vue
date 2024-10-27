<template>
  <div>Callback</div>
</template>
<script setup lang="ts">
// callback.ts
import { UserManager } from "oidc-client-ts";

import { useUserStore } from "../stores/user";
import { User } from "@/common/interfaces";
import * as env from "../app.config";
const userStorage = useUserStore();

// Configure the OIDC client
const userManager = new UserManager({
  authority: env.idpAuthority, // Replace with your OIDC provider authority
  client_id: env.idpClientId, // Replace with your actual client ID
  redirect_uri: `${window.location.origin}${env.idpRedirectPath}`, // Your redirect URI must match the OIDC provider settings
  response_type: "code",
  scope: env.idpScope, // Adjust as needed for your application's scopes
  post_logout_redirect_uri: `${window.location.origin}${env.idpLogoutRedirectPath}`,
});

(async () => {
  try {
    // Complete the sign-in by handling the callback
    const user = await userManager.signinRedirectCallback();
    console.log("user.access_token");

    const token = user.access_token;

    const newUser: User = {
      access_token: user.access_token,
      id_token: user.id_token || "",
      email: user.profile.email || "",
      preferred_username: user.profile.preferred_username || "",
      family_name: user.profile.family_name || "",
      given_name: user.profile.given_name || "",
    };

    userStorage.setUser(newUser);

    const fetchUsers = async () => {
      try {
        const response = await fetch(
          "http://localhost:8080/management/v1/info",
          {
            method: "GET", // HTTP method
            headers: {
              Authorization: `Bearer ${token}`, // Add Authorization header
              "Content-Type": "application/json", // Specify the content type
            },
          }
        );
        if (!response.ok) {
          throw new Error(`Error: ${response.statusText}`);
        }
        const data = await response.json();
        if (!data.bootstrapped) {
          try {
            const response_b = await fetch(
              "http://localhost:8080/management/v1/bootstrap",
              {
                method: "POST", // HTTP method
                headers: {
                  Authorization: `Bearer ${token}`, // Add Authorization header
                  "Content-Type": "application/json", // Specify the content type
                },
                body: JSON.stringify({ "accept-terms-of-use": true }),
              }
            );
            if (!response_b.ok) {
              throw new Error(`Error: ${response_b.statusText}`);
            }
          } catch (error) {
            console.error(error);
          }
        }
      } catch (err) {
        console.error(err);
      }
    };
    fetchUsers();
    // Redirect to the stored return URL, or to a default page
    //window.location.href = "/";
  } catch (error) {
    console.error("Error during callback processing:", error);
    // Optionally, redirect to an error page
    //window.location.href = "/error";
  }
})();
</script>
