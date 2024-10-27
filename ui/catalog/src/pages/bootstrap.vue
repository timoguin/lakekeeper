<template>
  <div class="scrollable-container"><EULA></EULA></div>
  <v-btn @click="bootstrap">Accept</v-btn>
</template>

<script lang="ts" setup>
import { useUserStore } from "../stores/user";
import { useVisualStore } from "../stores/visual";
import { icebergCatalogUrl } from "../app.config";
import { ProjectCatalog } from "../common/interfaces";
import router from "../router";

const userStore = useUserStore();
const user = userStore.getUser();
const visual = useVisualStore();

async function bootstrap() {
  try {
    console.log(user.access_token);
    const response = await fetch(
      `${icebergCatalogUrl}/management/v1/bootstrap`,
      {
        method: "POST", // HTTP method
        headers: {
          Authorization: `Bearer ${user.access_token}`, // Add Authorization header
          "Content-Type": "application/json", // Specify the content type
        },
        body: JSON.stringify({ "accept-terms-of-use": true }),
      }
    );
    if (!response.ok) {
      throw new Error(`Error: ${response.statusText}`);
    }
  } catch (error) {
    console.error(error);
  } finally {
    await getServerInfo();
  }
}

async function getServerInfo() {
  try {
    const response = await fetch(`${icebergCatalogUrl}/management/v1/info`, {
      method: "GET", // HTTP method
      headers: {
        Authorization: `Bearer ${user.access_token}`,
        "Content-Type": "application/json", // Specify the content type
      },
    });
    if (!response.ok) {
      throw new Error(`Error: ${response.statusText}`);
    }
    const data: ProjectCatalog = await response.json();
    visual.setProjectCatalog(data);
    router.push("/");
  } catch (error) {}
}
</script>

<style scoped>
.scrollable-container {
  max-width: 80%; /* Maximum width of the container */
  margin: auto; /* Center the container horizontally */
}

.scrollable-content {
  max-height: 400px; /* Adjust as needed for your layout */
  overflow-y: auto; /* Enable vertical scrolling */
  border: 1px solid #ccc; /* Optional: add a border for visibility */
  padding: 10px; /* Optional: add some padding */
}
</style>
