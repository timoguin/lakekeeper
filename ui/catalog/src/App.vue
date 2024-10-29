<template>
  <v-app>
    <v-main>
      <router-view />
    </v-main>
  </v-app>
</template>

<script lang="ts" setup>
import { onMounted } from "vue";
import { idpOn } from "@/app.config";
import router from "@/router";
import { useVisualStore } from "@/stores/visual";
import { useUserStore } from "@/stores/user";
import { ProjectCatalog } from "@/common/interfaces";
import { icebergCatalogUrl } from "@/app.config";
import { useAuth } from "@/plugins/auth";

const visual = useVisualStore();
const user = useUserStore();
const auth = useAuth();

// const baseUrl = env.icebergCatalogUrl as string;
// const managementUrl = baseUrl + "/management/v1";

onMounted(async () => {
  if (!idpOn) {
    await getServerInfo();

    if (!visual.project.bootstrapped) router.push("/bootstrap");
  }
});

if (user.isAuthenticated) {
  const checkTokenExpiry = async () => {
    try {
      const now = Math.floor(Date.now() / 1000); // Convert to seconds
      const timeLeft = user.getUser().token_expires_at - now;

      // If less than 1 minute remaining, refresh the token
      if (timeLeft <= 60) {
        await auth.refreshToken();
        // const at = (await auth.refreshToken()) || "";
        // console.log("at", at);
        // user.renewAT(at);
      }
    } catch (error) {
      console.error(error);
    }
  };

  setInterval(checkTokenExpiry, 60000);
}

async function getServerInfo() {
  try {
    const response = await fetch(`${icebergCatalogUrl}/management/v1/info`, {
      method: "GET", // HTTP method
      headers: {
        "Content-Type": "application/json", // Specify the content type
      },
    });
    if (!response.ok) {
      throw new Error(`Error: ${response.statusText}`);
    }
    const data: ProjectCatalog = await response.json();
    visual.setProjectCatalog(data);
  } catch (error) {}
}
</script>
