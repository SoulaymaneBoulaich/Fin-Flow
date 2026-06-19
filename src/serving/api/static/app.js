/* app.js — FinFlow Dashboard Engine */

document.addEventListener("DOMContentLoaded", () => {
    // ─── Clock Timer ────────────────────────────────────────────────────────
    setInterval(() => {
        const now = new Date();
        document.getElementById("clock").innerText = now.toUTCString().replace("GMT", "UTC");
    }, 1000);

    // ─── Constants & State ───────────────────────────────────────────────────
    const REFRESH_RATE = 4000; // 4 seconds
    let activeTicker = "AAPL";
    let historyCache = [];
    let tickerPriceData = {}; // Store latest ticker pricing for tape

    // DOM Elements
    const selector = document.getElementById("tickerSelector");
    const tape = document.getElementById("tickerTape");
    const overallHealth = document.getElementById("overallHealth");
    const servicesList = document.getElementById("servicesList");
    const metricLatest = document.getElementById("metricLatest");
    const metricRange = document.getElementById("metricRange");
    const metricVol = document.getElementById("metricVol");
    
    // Privacy Elements
    const piiName = document.getElementById("piiName");
    const piiEmail = document.getElementById("piiEmail");
    const piiAge = document.getElementById("piiAge");
    const btnApplyPrivacy = document.getElementById("btnApplyPrivacy");
    const privacyResult = document.getElementById("privacyResult");

    // ─── Initialize ─────────────────────────────────────────────────────────
    initApp();

    async function initApp() {
        // Fetch health and initial tickers list
        await updateHealth();
        await updateTickerTape();
        await fetchHistory(activeTicker);
        
        // Polling loop
        setInterval(async () => {
            await updateHealth();
            await updateTickerTape();
            await fetchLatestPrice(activeTicker);
        }, REFRESH_RATE);

        // Selector handler
        selector.addEventListener("change", (e) => {
            activeTicker = e.target.value;
            fetchHistory(activeTicker);
        });

        // GDPR Playground handler
        btnApplyPrivacy.addEventListener("click", applyPrivacyDemo);
        applyPrivacyDemo(); // Trigger initial view
    }

    // ─── Health Polling ──────────────────────────────────────────────────────
    async function updateHealth() {
        try {
            const res = await fetch("/health");
            if (!res.ok) throw new Error("API health unavailable");
            const data = await res.json();
            
            // Set Overall Health Badge
            if (data.status === "healthy") {
                overallHealth.className = "health-status-badge healthy";
                overallHealth.querySelector(".text").innerText = "ALL SYSTEMS HEALTHY";
            } else {
                overallHealth.className = "health-status-badge degraded";
                overallHealth.querySelector(".text").innerText = "SERVICES DEGRADED";
            }

            // Set Individual Service Badges
            for (const [srv, stat] of Object.entries(data.services)) {
                const srvEl = servicesList.querySelector(`[data-service="${srv}"]`);
                if (srvEl) {
                    const statusEl = srvEl.querySelector(".service-status");
                    statusEl.innerText = stat;
                    statusEl.className = `service-status ${stat}`;
                }
            }
        } catch (err) {
            console.error("Error fetching health:", err);
            overallHealth.className = "health-status-badge degraded";
            overallHealth.querySelector(".text").innerText = "DISCONNECTED";
        }
    }

    // ─── Ticker Tape Update ──────────────────────────────────────────────────
    async function updateTickerTape() {
        try {
            const res = await fetch("/tickers");
            if (!res.ok) throw new Error("Ticker catalog unavailable");
            const tickersList = await res.json();

            const tapeHtml = [];
            for (const t of tickersList) {
                // Mock changes for real-time vibe
                const mockPrice = await getLatestPriceFromCache(t.ticker);
                const mockDiff = (Math.random() * 2 - 1).toFixed(2);
                const isUp = mockDiff >= 0;
                
                tapeHtml.push(`
                    <div class="ticker-item">
                        <span class="symbol">${t.ticker}</span>
                        <span class="price">$${mockPrice}</span>
                        <span class="change ${isUp ? 'ticker-up' : 'ticker-down'}">
                            ${isUp ? '▲' : '▼'} ${isUp ? '+' : ''}${mockDiff}%
                        </span>
                    </div>
                `);
            }
            tape.innerHTML = tapeHtml.join("") + tapeHtml.join(""); // Duplicate for seamless scrolling loop
        } catch (err) {
            console.error("Error updating ticker tape:", err);
            tape.innerHTML = `<div class="ticker-item text-orange">Failed to load ticker tape</div>`;
        }
    }

    async function getLatestPriceFromCache(ticker) {
        if (tickerPriceData[ticker]) return tickerPriceData[ticker];
        
        try {
            const res = await fetch(`/tickers/${ticker}/latest`);
            if (res.ok) {
                const data = await res.json();
                tickerPriceData[ticker] = data.close.toFixed(2);
                return tickerPriceData[ticker];
            }
        } catch {}
        
        const fallbacks = { AAPL: 185.00, TSLA: 245.00, AMZN: 191.00, MSFT: 420.00, GOOGL: 175.00, NVDA: 900.00, META: 530.00, NFLX: 680.00 };
        return fallbacks[ticker] || 100.00;
    }

    // ─── Fetch Active Ticker Data & Draw Chart ───────────────────────────────
    async function fetchHistory(ticker) {
        try {
            const res = await fetch(`/tickers/${ticker}/history?limit=30`);
            if (!res.ok) throw new Error("Historical data unavailable");
            const data = await res.json();
            
            historyCache = data.sort((a, b) => new Date(a.date) - new Date(b.date));
            
            // Draw chart using history data
            drawCanvasChart();
            
            // Update latest metrics
            if (historyCache.length > 0) {
                const latest = historyCache[historyCache.length - 1];
                await updateUIWithLatest(ticker, latest);
            }
        } catch (err) {
            console.error("Error fetching ticker history:", err);
        }
    }

    async function fetchLatestPrice(ticker) {
        try {
            const res = await fetch(`/tickers/${ticker}/latest`);
            if (!res.ok) throw new Error("Latest price unavailable");
            const latest = await res.json();
            
            // Update last item in cache
            if (historyCache.length > 0) {
                historyCache[historyCache.length - 1] = {
                    ...historyCache[historyCache.length - 1],
                    close: latest.close,
                    high: Math.max(historyCache[historyCache.length - 1].high, latest.high),
                    low: Math.min(historyCache[historyCache.length - 1].low, latest.low),
                    volume: latest.volume,
                };
                drawCanvasChart();
                await updateUIWithLatest(ticker, latest);
            }
        } catch (err) {
            console.error("Error updates latest price:", err);
        }
    }

    function updateUIWithLatest(ticker, latest) {
        tickerPriceData[ticker] = latest.close.toFixed(2);
        metricLatest.innerText = `$${latest.close.toFixed(2)}`;
        
        const open = latest.open || latest.close * 0.99;
        const high = latest.high || latest.close * 1.01;
        const low = latest.low || latest.close * 0.99;
        metricRange.innerText = `$${low.toFixed(2)} - $${high.toFixed(2)}`;
        
        const volMB = (latest.volume / 1_000_000).toFixed(2);
        metricVol.innerText = `${volMB}M shares`;
    }

    // ─── Vanilla HTML5 Canvas Charting Engine ────────────────────────────────
    function drawCanvasChart() {
        const canvas = document.getElementById("stockChart");
        if (!canvas) return;
        
        const ctx = canvas.getContext("2d");
        const dpr = window.devicePixelRatio || 1;
        
        // Setup scaling for Retina displays
        const rect = canvas.getBoundingClientRect();
        canvas.width = rect.width * dpr;
        canvas.height = rect.height * dpr;
        ctx.scale(dpr, dpr);
        
        const width = rect.width;
        const height = rect.height;
        ctx.clearRect(0, 0, width, height);

        if (historyCache.length === 0) {
            ctx.fillStyle = "#94a3b8";
            ctx.font = "14px Outfit";
            ctx.textAlign = "center";
            ctx.fillText("No chart data available", width / 2, height / 2);
            return;
        }

        // Price mapping boundaries
        const prices = historyCache.map(d => d.close);
        const maxPrice = Math.max(...prices) * 1.02;
        const minPrice = Math.min(...prices) * 0.98;
        const range = maxPrice - minPrice;

        const points = historyCache.map((d, index) => {
            const x = (index / (historyCache.length - 1)) * (width - 60) + 30;
            const y = height - 40 - ((d.close - minPrice) / range) * (height - 80);
            return { x, y, date: d.date, close: d.close };
        });

        // 1. Draw glowing gradient line
        ctx.beginPath();
        ctx.moveTo(points[0].x, points[0].y);
        for (let i = 1; i < points.length; i++) {
            ctx.lineTo(points[i].x, points[i].y);
        }
        
        // Indigo line stroke
        ctx.strokeStyle = "#6366f1";
        ctx.lineWidth = 3;
        ctx.shadowColor = "rgba(99, 102, 241, 0.4)";
        ctx.shadowBlur = 10;
        ctx.stroke();
        ctx.shadowBlur = 0; // Reset shadow

        // 2. Draw area fill gradient
        ctx.beginPath();
        ctx.moveTo(points[0].x, points[0].y);
        for (let i = 1; i < points.length; i++) {
            ctx.lineTo(points[i].x, points[i].y);
        }
        ctx.lineTo(points[points.length - 1].x, height - 30);
        ctx.lineTo(points[0].x, height - 30);
        ctx.closePath();
        
        const fillGrad = ctx.createLinearGradient(0, 20, 0, height - 30);
        fillGrad.addColorStop(0, "rgba(99, 102, 241, 0.15)");
        fillGrad.addColorStop(1, "rgba(99, 102, 241, 0)");
        ctx.fillStyle = fillGrad;
        ctx.fill();

        // 3. Draw grid lines & axis labels
        ctx.strokeStyle = "rgba(255, 255, 255, 0.05)";
        ctx.lineWidth = 1;
        
        // Horizontal grid
        for (let i = 0; i <= 3; i++) {
            const y = 30 + i * ((height - 60) / 3);
            ctx.beginPath();
            ctx.moveTo(30, y);
            ctx.lineTo(width - 30, y);
            ctx.stroke();

            // Label
            const val = maxPrice - i * (range / 3);
            ctx.fillStyle = "#64748b";
            ctx.font = "10px Outfit";
            ctx.fillText(`$${val.toFixed(0)}`, width - 55, y - 5);
        }

        // Date labels (4 markers)
        ctx.fillStyle = "#64748b";
        ctx.font = "9px Outfit";
        ctx.textAlign = "center";
        const stride = Math.floor(points.length / 4);
        for (let i = 0; i < 4; i++) {
            const idx = Math.min(i * stride, points.length - 1);
            const pt = points[idx];
            // Format to MM/DD
            const dStr = pt.date.slice(5); 
            ctx.fillText(dStr, pt.x, height - 12);
        }
    }

    // ─── GDPR / Privacy Sandbox Simulation ───────────────────────────────────
    function applyPrivacyDemo() {
        const nameVal = piiName.value.trim() || "John Doe";
        const emailVal = piiEmail.value.trim() || "john@company.com";
        const ageVal = parseInt(piiAge.value) || 30;

        // Masking Simulation
        const emailParts = emailVal.split("@");
        const maskedEmail = emailParts[0].charAt(0) + "***" + "@" + (emailParts[1] || "gmail.com");
        
        const nameParts = nameVal.split(" ");
        const maskedName = nameParts.map(p => p.charAt(0) + "***").join(" ");

        // Pseudonymization key simulation (simple deterministic hash simulation)
        const pseudoId = simpleHash(emailVal);

        // Generalization
        const ageRange = `${Math.floor(ageVal / 10) * 10}-${Math.floor(ageVal / 10) * 10 + 9}`;

        const outputRecord = {
            "record_id": "row_" + simpleHash(nameVal).substring(0, 8),
            "user_pseudoid": pseudoId, // Join key
            "full_name": {
                "original": nameVal,
                "masked": maskedName,
                "strategy": "partial_mask"
            },
            "email": {
                "original": emailVal,
                "masked": maskedEmail,
                "strategy": "email_mask"
            },
            "age": {
                "original": ageVal,
                "generalization": ageRange,
                "strategy": "k-anonymity bucket"
            },
            "ingested_zone": "Silver (MinIO/Parquet)",
            "governance_tag": "PII_RESTRICTED"
        };

        privacyResult.innerText = JSON.stringify(outputRecord, null, 2);
    }

    function simpleHash(str) {
        // Simple deterministic hash simulation for dashboard demo
        let hash = 0;
        for (let i = 0; i < str.length; i++) {
            const char = str.charCodeAt(i);
            hash = (hash << 5) - hash + char;
            hash = hash & hash;
        }
        return "sha256_" + Math.abs(hash).toString(16).padStart(16, "0") + "ea8bc2c7";
    }
});
