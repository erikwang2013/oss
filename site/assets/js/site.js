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

  // Feature and architecture image lightbox.
  const imageDialog = document.getElementById("image-dialog");
  const imageLarge = document.getElementById("image-large");
  const imageTitle = document.getElementById("image-title");
  document.querySelectorAll("[data-image]").forEach((button) => {
    button.addEventListener("click", () => {
      if (!imageDialog || !imageLarge || !imageTitle) return;
      const image = button.getAttribute("data-image");
      const title = button.getAttribute("data-title") || "架构图";
      if (!image) return;
      imageLarge.src = image;
      imageLarge.alt = title;
      imageTitle.textContent = title;
      if (typeof imageDialog.showModal === "function") imageDialog.showModal();
      else imageDialog.setAttribute("open", "");
    });
  });
  imageDialog?.addEventListener("click", (event) => {
    if (event.target === imageDialog) imageDialog.close();
  });
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
