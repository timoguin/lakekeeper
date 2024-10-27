<template>
  <v-container class="fill-height">
    <v-responsive class="align-centerfill-height mx-auto" max-width="900">
      <div class="text-center">
        <v-img class="mb-4" height="100" src="@/assets/bear.png" />

        <div>
          <span class="text-h2 font-weight-bold">Lakekeeper</span>
        </div>
      </div>

      <div class="py-4" />
      <v-row
        ><v-col>
          <v-card class="mx-auto" max-width="344" outlined>
            <v-list-item three-line>
              <v-list-item-content>
                <div class="text-overline mb-4">WAREHOUSES</div>
                <v-list-item-title class="text-h5 mb-1">
                  Iceberg Tables
                </v-list-item-title>
                <v-list-item-subtitle
                  >Create and manage your Warhouses, Tables and Access
                  Controls</v-list-item-subtitle
                >
              </v-list-item-content>

              <v-list-item-avatar
                tile
                size="80"
                color="grey"
              ></v-list-item-avatar>
            </v-list-item>

            <v-card-actions>
              <v-btn outlined rounded to="/warehouses"> Open </v-btn>
            </v-card-actions>
          </v-card></v-col
        ><v-col>
          <v-card class="mx-auto" max-width="344" outlined>
            <v-list-item three-line>
              <v-list-item-content>
                <div class="text-overline mb-4">VOLUMES</div>
                <v-list-item-title class="text-h5 mb-1">
                  Files & Objects
                </v-list-item-title>
                <v-list-item-subtitle
                  >Store and govern Access to your Files and Objects to run your
                  AI and ML processes
                </v-list-item-subtitle>
              </v-list-item-content>

              <v-list-item-avatar
                tile
                size="80"
                color="grey"
              ></v-list-item-avatar>
            </v-list-item>

            <v-card-actions>
              <v-btn outlined rounded @click="showSnackbar"> Open </v-btn>
            </v-card-actions>
          </v-card>
        </v-col></v-row
      >
      <v-snackbar v-model="snackbarVisible" timeout="3000">
        Volumes are not active yet
      </v-snackbar>
    </v-responsive>
  </v-container>
</template>

<script setup lang="ts">
//
const snackbarVisible = ref(false);

function showSnackbar() {
  snackbarVisible.value = true;
}
</script>
