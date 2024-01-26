import { defineConfig } from 'vite'
import solid from 'vite-plugin-solid'
import UnoCSS from "unocss/vite";

export default defineConfig({
  plugins: [solid(), UnoCSS()],
  build: {
    outDir: "../dist"
  },
  server: {
      proxy: {
        "/v1": {
          target: "http://localhost:9876",
          changeOrigin: true,
        },
      },
    },
})
