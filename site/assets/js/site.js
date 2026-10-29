(() => {
  "use strict";

  const reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  // Animated SVG pets have a still PNG for reduced-motion users.
  if (reduced) {
    document.querySelectorAll("img[data-static]").forEach((image) => {
      image.src = image.dataset.static;
    });
  }

  // Mobile navigation
  const menuButton = document.querySelector(".menu-toggle");
  const mobileNav = document.getElementById("mobile-nav");
  if (menuButton && mobileNav) {
    menuButton.addEventListener("click", () => {
      const open = mobileNav.classList.toggle("is-open");
      menuButton.setAttribute("aria-expanded", String(open));
      menuButton.textContent = open ? "关闭" : "菜单";
      mobileNav.hidden = !open;
    });
    mobileNav.addEventListener("click", (event) => {
      if (event.target.closest("a")) {
        mobileNav.classList.remove("is-open");
        mobileNav.hidden = true;
        menuButton.setAttribute("aria-expanded", "false");
        menuButton.textContent = "菜单";
      }
    });
  }

  // Header state and back-to-top via observers, no scroll handlers.
  const header = document.querySelector(".site-header");
  const toTop = document.getElementById("to-top");
  const sentinel = document.getElementById("top-sentinel");
  if ("IntersectionObserver" in window && sentinel) {
    const observer = new IntersectionObserver(
      ([entry]) => {
        header?.classList.toggle("is-scrolled", !entry.isIntersecting);
      },
      { rootMargin: "-64px 0px 0px 0px", threshold: 0 }
    );
    observer.observe(sentinel);
  }
  const hero = document.querySelector(".hero, .doc-hero");
  if ("IntersectionObserver" in window && hero && toTop) {
    const topObserver = new IntersectionObserver(
      ([entry]) => {
        toTop.hidden = entry.isIntersecting;
      },
      { threshold: 0 }
    );
    topObserver.observe(hero);
  }

  toTop?.addEventListener("click", () => {
    window.scrollTo({ top: 0, behavior: "instant" });
  });

  // Copy buttons
  const copyStatus = document.createElement("div");
  copyStatus.className = "sr-only";
  copyStatus.setAttribute("aria-live", "polite");
  document.body.append(copyStatus);

  document.addEventListener("click", async (event) => {
    const button = event.target.closest("[data-copy]");
    if (!button) return;
    const selector = button.getAttribute("data-copy");
    const source = selector ? document.querySelector(selector) : null;
    const text = source ? source.innerText : "";
    if (!text) return;
    try {
      await navigator.clipboard.writeText(text);
      const original = button.textContent;
      button.textContent = "已复制";
      copyStatus.textContent = "已复制到剪贴板";
      setTimeout(() => {
        button.textContent = original;
      }, 1400);
    } catch {
      button.textContent = "复制失败";
      copyStatus.textContent = "复制失败，请手动选择文本";
      setTimeout(() => {
        button.textContent = "复制";
      }, 1400);
    }
  });

  // Code tabs
  document.querySelectorAll("[data-tabs]").forEach((tabs) => {
    const buttons = tabs.querySelectorAll(".tab");
    const panelWrap = tabs.parentElement;
    buttons.forEach((button) => {
      button.addEventListener("click", () => {
        const target = button.getAttribute("aria-controls");
        buttons.forEach((other) => {
          other.setAttribute(
            "aria-selected",
            String(other === button)
          );
        });
        panelWrap?.querySelectorAll(".tab-panel").forEach((panel) => {
          panel.hidden = panel.id !== target;
        });
      });
    });
  });

  // Donation QR modal
  const dialog = document.getElementById("qr-dialog");
  const dialogImage = document.getElementById("qr-large");
  const dialogTitle = document.getElementById("qr-title");
  document.querySelectorAll("[data-qr]").forEach((button) => {
    button.addEventListener("click", () => {
      if (!dialog || !dialogImage || !dialogTitle) return;
      const image = button.getAttribute("data-qr");
      const title = button.getAttribute("data-title") || "打赏二维码";
      if (!image) return;
      dialogImage.src = image;
      dialogImage.alt = title;
      dialogTitle.textContent = title;
      if (typeof dialog.showModal === "function") dialog.showModal();
      else dialog.setAttribute("open", "");
    });
  });
  dialog?.addEventListener("click", (event) => {
    if (event.target === dialog) dialog.close();
  });
  document.querySelectorAll("[data-close-dialog]").forEach((button) => {
    button.addEventListener("click", () => dialog?.close());
  });

  // Feature and architecture image lightbox: click to open, then zoom/pan in the dialog.
  const imageDialog = document.getElementById("image-dialog");
  const imageLarge = document.getElementById("image-large");
  const imageTitle = document.getElementById("image-title");
  const imageStage = imageDialog?.querySelector("[data-image-stage]") || null;
  const ZOOM_STEP = 1.25;
  const ZOOM_MIN = 1;
  const ZOOM_MAX = 6;
  const zoomState = {
    zoom: 1, // 用户缩放倍数（1 = 适配舞台）
    fit: 1, // 适配舞台的基础缩放
    panX: 0,
    panY: 0,
    drag: null,
    pinch: null,
    pointers: new Map(),
  };

  const applyZoom = () => {
    if (!imageLarge) return;
    imageLarge.style.transform =
      `translate(-50%, -50%) translate(${zoomState.panX}px, ${zoomState.panY}px) ` +
      `scale(${zoomState.fit * zoomState.zoom})`;
  };

  const clampPan = () => {
    if (!imageLarge || !imageStage) return;
    const iw = imageLarge.naturalWidth || 0;
    const ih = imageLarge.naturalHeight || 0;
    const scale = zoomState.fit * zoomState.zoom;
    const maxX = Math.max(0, (iw * scale - imageStage.clientWidth) / 2);
    const maxY = Math.max(0, (ih * scale - imageStage.clientHeight) / 2);
    zoomState.panX = Math.max(-maxX, Math.min(maxX, zoomState.panX));
    zoomState.panY = Math.max(-maxY, Math.min(maxY, zoomState.panY));
  };

  const resetZoom = () => {
    zoomState.zoom = 1;
    zoomState.panX = 0;
    zoomState.panY = 0;
    if (imageLarge && imageStage && imageLarge.naturalWidth && imageLarge.naturalHeight) {
      zoomState.fit = Math.min(
        imageStage.clientWidth / imageLarge.naturalWidth,
        imageStage.clientHeight / imageLarge.naturalHeight,
        1
      );
    } else {
      zoomState.fit = 1;
    }
    applyZoom();
  };

  // cx/cy = 舞台中心为原点的光标/双指中点坐标；缩放时保持该点稳定。
  const setZoom = (next, cx = 0, cy = 0) => {
    const from = zoomState.zoom;
    const to = Math.max(ZOOM_MIN, Math.min(ZOOM_MAX, next));
    if (!imageLarge || !imageStage || Math.abs(to - from) < 1e-6) return;
    const k = to / from;
    zoomState.panX = cx - (cx - zoomState.panX) * k;
    zoomState.panY = cy - (cy - zoomState.panY) * k;
    zoomState.zoom = to;
    clampPan();
    applyZoom();
  };

  const stagePoint = (event) => {
    if (!imageStage) return { x: 0, y: 0 };
    const rect = imageStage.getBoundingClientRect();
    return {
      x: event.clientX - rect.left - rect.width / 2,
      y: event.clientY - rect.top - rect.height / 2,
    };
  };

  document.querySelectorAll("[data-image]").forEach((button) => {
    button.addEventListener("click", () => {
      if (!imageDialog || !imageLarge || !imageTitle) return;
      const image = button.getAttribute("data-image");
      const title = button.getAttribute("data-title") || "架构图";
      if (!image) return;
      imageLarge.src = image;
      imageLarge.alt = title;
      imageTitle.textContent = title;
      zoomState.pointers.clear();
      zoomState.drag = null;
      zoomState.pinch = null;
      zoomState.zoom = 1;
      zoomState.fit = 1;
      zoomState.panX = 0;
      zoomState.panY = 0;
      imageLarge.style.transform = "";
      if (typeof imageDialog.showModal === "function") imageDialog.showModal();
      else imageDialog.setAttribute("open", "");
      const fit = () => requestAnimationFrame(resetZoom);
      if (imageLarge.complete) fit();
      else imageLarge.addEventListener("load", fit, { once: true });
    });
  });

  imageDialog?.querySelectorAll("[data-zoom-in]").forEach((button) => {
    button.addEventListener("click", () => setZoom(zoomState.zoom * ZOOM_STEP));
  });
  imageDialog?.querySelectorAll("[data-zoom-out]").forEach((button) => {
    button.addEventListener("click", () => setZoom(zoomState.zoom / ZOOM_STEP));
  });
  imageDialog?.querySelectorAll("[data-zoom-reset]").forEach((button) => {
    button.addEventListener("click", resetZoom);
  });

  imageStage?.addEventListener(
    "wheel",
    (event) => {
      event.preventDefault();
      const p = stagePoint(event);
      setZoom(zoomState.zoom * Math.exp(-event.deltaY * 0.0015), p.x, p.y);
    },
    { passive: false }
  );

  imageStage?.addEventListener("dblclick", (event) => {
    event.preventDefault();
    if (zoomState.zoom > 1.01) {
      resetZoom();
    } else {
      const p = stagePoint(event);
      setZoom(2, p.x, p.y);
    }
  });

  imageStage?.addEventListener("pointerdown", (event) => {
    if (event.pointerType === "mouse" && event.button !== 0) return;
    imageStage.setPointerCapture?.(event.pointerId);
    zoomState.pointers.set(event.pointerId, { x: event.clientX, y: event.clientY });
    if (zoomState.pointers.size === 1) {
      zoomState.drag = {
        x: event.clientX,
        y: event.clientY,
        panX: zoomState.panX,
        panY: zoomState.panY,
      };
      imageStage.classList.add("is-dragging");
    } else if (zoomState.pointers.size === 2) {
      const [a, b] = [...zoomState.pointers.values()];
      zoomState.pinch = {
        dist: Math.hypot(a.x - b.x, a.y - b.y) || 1,
        zoom: zoomState.zoom,
      };
      zoomState.drag = null;
    }
  });

  imageStage?.addEventListener("pointermove", (event) => {
    if (!zoomState.pointers.has(event.pointerId)) return;
    zoomState.pointers.set(event.pointerId, { x: event.clientX, y: event.clientY });
    if (zoomState.pointers.size >= 2 && zoomState.pinch) {
      const [a, b] = [...zoomState.pointers.values()].slice(0, 2);
      const rect = imageStage.getBoundingClientRect();
      const dist = Math.hypot(a.x - b.x, a.y - b.y) || 1;
      const mid = {
        x: (a.x + b.x) / 2 - rect.left - rect.width / 2,
        y: (a.y + b.y) / 2 - rect.top - rect.height / 2,
      };
      setZoom(zoomState.pinch.zoom * (dist / zoomState.pinch.dist), mid.x, mid.y);
    } else if (zoomState.drag) {
      zoomState.panX = zoomState.drag.panX + (event.clientX - zoomState.drag.x);
      zoomState.panY = zoomState.drag.panY + (event.clientY - zoomState.drag.y);
      clampPan();
      applyZoom();
    }
  });

  const endImagePointer = (event) => {
    if (!imageStage) return;
    zoomState.pointers.delete(event.pointerId);
    if (zoomState.pointers.size === 0) {
      zoomState.drag = null;
      zoomState.pinch = null;
      imageStage.classList.remove("is-dragging");
    } else if (zoomState.pointers.size === 1) {
      const [p] = zoomState.pointers.values();
      zoomState.drag = { x: p.x, y: p.y, panX: zoomState.panX, panY: zoomState.panY };
      zoomState.pinch = null;
    }
  };
  imageStage?.addEventListener("pointerup", endImagePointer);
  imageStage?.addEventListener("pointercancel", endImagePointer);

  imageDialog?.addEventListener("keydown", (event) => {
    if (event.key === "+" || event.key === "=") {
      event.preventDefault();
      setZoom(zoomState.zoom * ZOOM_STEP);
    } else if (event.key === "-") {
      event.preventDefault();
      setZoom(zoomState.zoom / ZOOM_STEP);
    } else if (event.key === "0") {
      event.preventDefault();
      resetZoom();
    }
  });

  imageDialog?.addEventListener("click", (event) => {
    if (event.target === imageDialog) imageDialog.close();
  });
  imageDialog?.addEventListener("close", resetZoom);
  document.querySelectorAll("[data-close-image]").forEach((button) => {
    button.addEventListener("click", () => imageDialog?.close());
  });

  // Reveal on scroll. Content is already in the DOM for SEO and no-JS readers.
  if (!reduced && "IntersectionObserver" in window) {
    const reveal = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            entry.target.classList.add("is-visible");
            reveal.unobserve(entry.target);
          }
        });
      },
      { rootMargin: "0px 0px -8% 0px", threshold: 0.12 }
    );
    document.querySelectorAll("[data-reveal]").forEach((element) => {
      reveal.observe(element);
    });
  } else {
    document.querySelectorAll("[data-reveal]").forEach((element) => {
      element.classList.add("is-visible");
    });
  }

  document.querySelectorAll("[data-year]").forEach((element) => {
    element.textContent = String(new Date().getFullYear());
  });
})();
