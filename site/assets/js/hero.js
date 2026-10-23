// Hero WebGL scene. Three.js is loaded only after first paint, only on wide
// viewports, and never for reduced-motion users. The page is complete without it.

const canvas = document.getElementById("hero-canvas");
const stage = document.querySelector(".hero-stage");
const reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
const wide = window.matchMedia("(min-width: 900px)").matches;

if (canvas && stage && !reduced && wide && "WebGLRenderingContext" in window) {
  const load = async () => {
    try {
      const THREE = await import("./vendor/three.module.min.js");
      const renderer = new THREE.WebGLRenderer({
        canvas,
        alpha: true,
        antialias: false,
        powerPreference: "low-power",
      });
      renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 1.5));
      renderer.outputColorSpace = THREE.SRGBColorSpace;

      const scene = new THREE.Scene();
      const camera = new THREE.PerspectiveCamera(42, 1, 0.1, 100);
      camera.position.set(0, 0, 7.2);

      const group = new THREE.Group();
      scene.add(group);

      const palette = [0x2dd4bf, 0x60a5fa, 0x4ade80];
      const positions = new Float32Array(720 * 3);
      const colors = new Float32Array(720 * 3);
      const color = new THREE.Color();

      for (let i = 0; i < 720; i += 1) {
        const radius = 2.1 + Math.random() * 2.8;
        const theta = Math.random() * Math.PI * 2;
        const phi = Math.acos(2 * Math.random() - 1);
        positions[i * 3] = radius * Math.sin(phi) * Math.cos(theta);
        positions[i * 3 + 1] = radius * Math.sin(phi) * Math.sin(theta) * 0.7;
        positions[i * 3 + 2] = radius * Math.cos(phi);
        color.setHex(palette[i % palette.length]);
        colors[i * 3] = color.r;
        colors[i * 3 + 1] = color.g;
        colors[i * 3 + 2] = color.b;
      }

      const pointsGeometry = new THREE.BufferGeometry();
      pointsGeometry.setAttribute(
        "position",
        new THREE.BufferAttribute(positions, 3)
      );
      pointsGeometry.setAttribute("color", new THREE.BufferAttribute(colors, 3));
      const points = new THREE.Points(
        pointsGeometry,
        new THREE.PointsMaterial({
          size: 0.035,
          vertexColors: true,
          transparent: true,
          opacity: 0.8,
          depthWrite: false,
          blending: THREE.AdditiveBlending,
          sizeAttenuation: true,
        })
      );
      group.add(points);

      const boxes = [];
      const boxGeometry = new THREE.BoxGeometry(0.28, 0.28, 0.28);
      for (let i = 0; i < 7; i += 1) {
        const edges = new THREE.EdgesGeometry(boxGeometry);
        const line = new THREE.LineSegments(
          edges,
          new THREE.LineBasicMaterial({
            color: palette[i % palette.length],
            transparent: true,
            opacity: 0.4,
          })
        );
        const angle = (i / 7) * Math.PI * 2;
        const radius = 2.1 + (i % 3) * 0.6;
        line.position.set(
          Math.cos(angle) * radius,
          (Math.random() - 0.5) * 2.2,
          Math.sin(angle) * radius * 0.55
        );
        line.rotation.set(Math.random(), Math.random(), Math.random());
        line.userData = {
          speed: 0.15 + Math.random() * 0.25,
          base: line.position.y,
          phase: Math.random() * Math.PI * 2,
        };
        boxes.push(line);
        group.add(line);
      }

      const resize = () => {
        const rect = stage.getBoundingClientRect();
        const width = Math.max(1, Math.round(rect.width));
        const height = Math.max(1, Math.round(rect.height));
        renderer.setSize(width, height, false);
        camera.aspect = width / height;
        camera.updateProjectionMatrix();
      };
      resize();

      const observer = new ResizeObserver(resize);
      observer.observe(stage);

      let pointerX = 0;
      let pointerY = 0;
      let targetX = 0;
      let targetY = 0;
      window.addEventListener(
        "pointermove",
        (event) => {
          targetX = (event.clientX / window.innerWidth - 0.5) * 2;
          targetY = (event.clientY / window.innerHeight - 0.5) * 2;
        },
        { passive: true }
      );

      const clock = new THREE.Clock();
      let frameId = 0;
      const animate = () => {
        const time = clock.getElapsedTime();
        pointerX += (targetX - pointerX) * 0.04;
        pointerY += (targetY - pointerY) * 0.04;
        group.rotation.y = time * 0.055 + pointerX * 0.16;
        group.rotation.x = pointerY * 0.12;
        boxes.forEach((box) => {
          box.rotation.x += 0.0018 * box.userData.speed * 60;
          box.rotation.y += 0.0024 * box.userData.speed * 60;
          box.position.y =
            box.userData.base + Math.sin(time * box.userData.speed + box.userData.phase) * 0.18;
        });
        renderer.render(scene, camera);
        frameId = requestAnimationFrame(animate);
      };

      const start = () => {
        if (frameId) return;
        stage.classList.add("is-ready");
        frameId = requestAnimationFrame(animate);
      };
      const stop = () => {
        if (!frameId) return;
        cancelAnimationFrame(frameId);
        frameId = 0;
      };
      document.addEventListener("visibilitychange", () => {
        if (document.hidden) stop();
        else start();
      });
      start();
    } catch {
      // Keep the CSS stage and mascot; WebGL is an enhancement only.
      stage.classList.remove("is-ready");
    }
  };

  const idle = window.requestIdleCallback || ((callback) => setTimeout(callback, 600));
  if (document.readyState === "complete") idle(load);
  else window.addEventListener("load", () => idle(load), { once: true });
}
