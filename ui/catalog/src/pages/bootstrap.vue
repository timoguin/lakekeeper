<template>
  <div v-if="!visual.projectInfo.bootstrapped">
    <v-row>
      <v-col cols="10" offset="1">
        <v-stepper :items="['Global Admin', 'EULA', 'Submit']">
          <template v-slot:item.1>
            <v-card flat>
              <v-card-title>
                Welcome {{ user.given_name }} {{ user.family_name }}
              </v-card-title>
              <v-card-text>
                Welcome to the initial setup for your system! As part of this
                setup, you'll create a Global Admin—a key user with full
                permissions to configure and manage your platform's settings and
                users.
              </v-card-text>
            </v-card>
          </template>

          <template v-slot:item.2>
            <v-card flat>
              <div style="max-height: 50vh; overflow-y: auto">
                <EULA></EULA>
              </div>
            </v-card>
          </template>

          <template v-slot:item.3>
            <v-card flat>
              <v-card-title>Consent</v-card-title>
              <v-card-text
                >By proceeding, you acknowledge that you have read, understood,
                and agree to the terms and conditions of the End User License
                Agreement (EULA).</v-card-text
              >
              <v-card-actions
                ><v-btn @click="bootstrap" class="mb-6"
                  >Accept</v-btn
                ></v-card-actions
              >
            </v-card>
          </template>
        </v-stepper>
      </v-col>
    </v-row>

    <div class="scrollable-container"></div>
  </div>
</template>

<script lang="ts" setup>
import { onBeforeMount } from "vue";
import { useUserStore } from "../stores/user";
import { useVisualStore } from "../stores/visual";
import { icebergCatalogUrl } from "../app.config";
import { ProjectCatalog } from "../common/interfaces";
import router from "../router";

const userStore = useUserStore();
const user = userStore.getUser();
const visual = useVisualStore();

onBeforeMount(async () => {
  await getServerInfo();
});

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
    if (visual.project.bootstrapped) router.push("/");
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
