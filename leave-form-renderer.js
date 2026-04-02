(function () {
    const PAGE = {
        width: 595.32,
        height: 841.92,
        // 完美校準並鎖定 Office Copy 的絕對位置 (354.33 + 215.00 = 569.33)
        officeOffset: 215.00, 
        // 獨立微調 Office Copy 的打勾與總節數，讓它完全不被上聯影響
        officeReasonYAdjust: 2.9, // 上來 0.1 (3 - 0.1 = 2.9)
        officePeriodsYAdjust: 3,
        studentFields: {
            // 第一頁保持不變
            className: { x: 138.09, y: 354.33, width: 32 },
            studentId: { x: 206.36, y: 354.33, width: 52 },
            studentName: { x: 305.94, y: 354.33, width: 114 },
            englishName: { x: 462.27, y: 354.33, width: 126 },
        },
        reasonMarks: {
            // 事假與病假保持不動
            "Personal Leave": { x: 145.53, y: 380.35 },
            // 身心調適假 靠右 0.3 格 (282.34 + 4.2 = 286.54)
            "Mental Health Leave": { x: 286.54, y: 379.75 },
            "Sick Leave": { x: 145.53, y: 397.27 },
            // 喪假 靠右 0.3 格 (282.94 + 4.2 = 287.14)
            "Funeral Leave": { x: 287.14, y: 397.39 },
        },
        timeFields: {
            month: 176.68,
            day: 225.38,
            hour: 309.09,
            minute: 359.94,
            // 第一頁保持不變
            fromY: 423.63,
            toY: 443.68,
        },
        // Student Total 下來 0.2 (457.53 + 0.2 = 457.73)
        periods: { x: 479.1, y: 457.73, width: 24 }, 
        note: {
            // 靠右 1 格 (50 + 14 = 64)
            x: 64,
            // 下去 0.5 格 (行高 18 * 0.5 = 9，594 + 9 = 603)
            y: 603, 
            maxWidth: 456, // 配合起點右移，再次微調最大寬度保持右側邊界對齊
            lineHeight: 18,
            maxUnits: 58,
            maxLines: 22,
        },
    };

    function esc(value) {
        return String(value ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;");
    }

    function formatNumber(value) {
        if (value === undefined || value === null || value === "") return "";
        return String(value);
    }

    function estimateTextWidth(text, size) {
        let width = 0;
        for (const ch of String(text || "")) {
            if (/[\u1100-\u11ff\u2e80-\u9fff\uf900-\ufaff]/.test(ch)) width += size * 0.98;
            else if (/[A-Z0-9]/.test(ch)) width += size * 0.66;
            else if (/[a-z]/.test(ch)) width += size * 0.54;
            else if (ch === " ") width += size * 0.28;
            else width += size * 0.4;
        }
        return width;
    }

    function textLengthAttr(text, width, size) {
        if (!width || estimateTextWidth(text, size) <= width) return "";
        return ' textLength="' + width + '" lengthAdjust="spacingAndGlyphs"';
    }

    function svgText(text, x, y, opts) {
        if (!text) return "";
        const options = opts || {};
        const anchor = options.anchor || "middle";
        const size = options.size || 11;
        const width = options.width || 0;
        const weight = options.weight || "600";
        const italic = Boolean(options.italic);
        const cls = options.cls || "leave-overlay-text";

        return '<text class="' + cls + '" x="' + x + '" y="' + y + '" text-anchor="' + anchor +
            '" font-size="' + size + '" font-weight="' + weight + '"' +
            (italic ? ' font-style="italic"' : "") +
            textLengthAttr(text, width, size) + ">" + esc(text) + "</text>";
    }

    function svgCheck(x, y) {
        return '<path d="M ' + (x - 4.2) + " " + (y + 0.8) + " L " + (x - 1.2) + " " + (y + 4.1) +
            " L " + (x + 5.6) + " " + (y - 4.8) +
            '" fill="none" stroke="#111827" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"></path>';
    }

    function visualUnits(ch) {
        return /[\u1100-\u11ff\u2e80-\u9fff\uf900-\ufaff]/.test(ch) ? 2 : 1;
    }

    function wrapText(text, maxUnits) {
        const lines = [];
        let current = "";
        let units = 0;

        for (const ch of text) {
            if (ch === "\n") {
                if (current.trim()) lines.push(current.trim());
                current = "";
                units = 0;
                continue;
            }

            const add = visualUnits(ch);
            if (units + add > maxUnits && current.trim()) {
                lines.push(current.trim());
                current = ch;
                units = add;
                continue;
            }

            current += ch;
            units += add;
        }

        if (current.trim()) lines.push(current.trim());
        return lines;
    }

    function buildNoteLines(reason) {
        const text = String(reason || "").trim();
        if (!text) return [];

        const parts = text.split(/\r?\n+/).map(s => s.trim()).filter(Boolean);
        const lines = [];

        parts.forEach((part, index) => {
            wrapText(part, PAGE.note.maxUnits).forEach(line => lines.push(line));
            if (index < parts.length - 1) lines.push("");
        });

        return lines.slice(0, PAGE.note.maxLines);
    }

    function renderCopy(record, startDate, endDate, offsetY) {
        const studentId = record.studentId || record.seatNo || record.studentNo || "";
        const chineseName = record.chineseName || record.studentName || record.name || "";
        const englishName = record.englishName || "";
        const className = record.className || record.studentClass || "";
        const fields = PAGE.studentFields;
        const time = PAGE.timeFields;
        const parts = [];

        parts.push(svgText(className, fields.className.x, fields.className.y + offsetY, {
            size: 10.8,
            width: fields.className.width,
        }));
        parts.push(svgText(studentId, fields.studentId.x, fields.studentId.y + offsetY, {
            size: 10.8,
            width: fields.studentId.width,
        }));
        parts.push(svgText(chineseName, fields.studentName.x, fields.studentName.y + offsetY, {
            size: 11.2,
            width: fields.studentName.width,
        }));
        parts.push(svgText(englishName, fields.englishName.x, fields.englishName.y + offsetY, {
            size: 10.6,
            width: fields.englishName.width,
        }));

        // 建立超強相容性判斷：無論 index.html 存入什麼值，都能自動模糊匹配對應的假別
        const rawReason = String(record.leaveType || record.leaveReason || "").trim().toLowerCase();
        let matchedReason = "";
        
        if (rawReason.includes("personal") || rawReason.includes("事")) {
            matchedReason = "Personal Leave";
        } else if (rawReason.includes("mental") || rawReason.includes("心理")) {
            matchedReason = "Mental Health Leave";
        } else if (rawReason.includes("sick") || rawReason.includes("health") || rawReason.includes("病")) {
            matchedReason = "Sick Leave";
        } else if (rawReason.includes("funeral") || rawReason.includes("喪")) {
            matchedReason = "Funeral Leave";
        } else {
            matchedReason = record.leaveType || record.leaveReason || "";
        }

        Object.entries(PAGE.reasonMarks).forEach(([type, pos]) => {
            if (matchedReason === type) {
                // 如果是印下聯 (offsetY > 0)，套用 office 專屬的打勾偏移量
                const yAdjust = (offsetY > 0 && PAGE.officeReasonYAdjust) ? PAGE.officeReasonYAdjust : 0;
                parts.push(svgCheck(pos.x, pos.y + offsetY + yAdjust));
            }
        });

        parts.push(svgText(formatNumber(startDate.getMonth() + 1), time.month, time.fromY + offsetY, { size: 11, width: 20 }));
        parts.push(svgText(formatNumber(startDate.getDate()), time.day, time.fromY + offsetY, { size: 11, width: 20 }));
        parts.push(svgText(formatNumber(String(startDate.getHours()).padStart(2, "0")), time.hour, time.fromY + offsetY, { size: 11, width: 22 }));
        parts.push(svgText(formatNumber(String(startDate.getMinutes()).padStart(2, "0")), time.minute, time.fromY + offsetY, { size: 11, width: 24 }));

        parts.push(svgText(formatNumber(endDate.getMonth() + 1), time.month, time.toY + offsetY, { size: 11, width: 20 }));
        parts.push(svgText(formatNumber(endDate.getDate()), time.day, time.toY + offsetY, { size: 11, width: 20 }));
        parts.push(svgText(formatNumber(String(endDate.getHours()).padStart(2, "0")), time.hour, time.toY + offsetY, { size: 11, width: 22 }));
        parts.push(svgText(formatNumber(String(endDate.getMinutes()).padStart(2, "0")), time.minute, time.toY + offsetY, { size: 11, width: 24 }));

        const periodYAdjust = (offsetY > 0 && PAGE.officePeriodsYAdjust) ? PAGE.officePeriodsYAdjust : 0;
        parts.push(svgText(formatNumber(record.periods || 1), PAGE.periods.x, PAGE.periods.y + offsetY + periodYAdjust, {
            size: 12.5,
            width: PAGE.periods.width,
            weight: "700",
        }));

        return parts.join("");
    }

    function renderNotes(record) {
        const lines = buildNoteLines(record.reason || record.parentNote);
        if (!lines.length) return "";

        return lines.map((line, index) => {
            if (!line) return "";
            return svgText(line, PAGE.note.x, PAGE.note.y + index * PAGE.note.lineHeight, {
                anchor: "start",
                size: 13.6,
                width: PAGE.note.maxWidth,
                weight: "500",
                cls: "leave-overlay-note",
            });
        }).join("");
    }

    function recordError(record) {
        if (!record) return "Leave record not found. It may have been deleted.";
        if (!record.startTime || !record.endTime) return "This leave record is missing its time range.";

        const startDate = new Date(record.startTime);
        const endDate = new Date(record.endTime);
        if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime())) {
            return "This leave record contains an invalid time range.";
        }

        return "";
    }

    function imageTag(primarySrc, fallbackSrc, alt) {
        return '<img class="leave-sheet-bg" src="' + esc(primarySrc) +
            '" data-fallback-src="' + esc(fallbackSrc) +
            '" alt="' + esc(alt) +
            '" onerror="if(this.dataset.fallbackSrc && this.dataset.fallbackSrc !== this.getAttribute(\'src\')){this.setAttribute(\'src\', this.dataset.fallbackSrc);}">';
    }

    function buildMarkup(record) {
        const startDate = new Date(record.startTime);
        const endDate = new Date(record.endTime);

        return '<div class="leave-sheet-stack">' +
            '<div class="leave-sheet">' +
                imageTag("assets/leave-template-1.png", "leave-template-1.png", "Leave form template page 1") +
                '<svg class="leave-sheet-svg" viewBox="0 0 ' + PAGE.width + " " + PAGE.height + '" aria-hidden="true" preserveAspectRatio="none">' +
                    renderCopy(record, startDate, endDate, 0) +
                    renderCopy(record, startDate, endDate, PAGE.officeOffset) +
                "</svg>" +
            "</div>" +
            '<div class="leave-sheet">' +
                imageTag("assets/leave-template-2.png", "leave-template-2.png", "Leave form template page 2") +
                '<svg class="leave-sheet-svg" viewBox="0 0 ' + PAGE.width + " " + PAGE.height + '" aria-hidden="true" preserveAspectRatio="none">' +
                    renderNotes(record) +
                "</svg>" +
            "</div>" +
        "</div>";
    }

    function buildEmptyMarkup(message, options) {
        const opts = options || {};
        const href = opts.emptyHref || "";
        const linkLabel = opts.emptyLinkLabel || "Back";
        const link = href
            ? '<p class="leave-empty-link"><a href="' + esc(href) + '">' + esc(linkLabel) + "</a></p>"
            : "";

        return '<div class="leave-empty">' +
            "<h2>&#x26A0;&#xFE0F; Cannot Load Form</h2>" +
            "<p>" + esc(message) + "</p>" +
            link +
        "</div>";
    }

    function renderInto(container, record, options) {
        if (!container) return false;
        const error = recordError(record);
        container.innerHTML = error ? buildEmptyMarkup(error, options) : buildMarkup(record);
        return !error;
    }

    function titleFor(record) {
        return "Leave Form - " + (record?.chineseName || record?.studentName || record?.studentId || "Student");
    }

    window.LeaveFormRenderer = {
        page: PAGE,
        recordError,
        buildMarkup,
        buildEmptyMarkup,
        renderInto,
        titleFor,
    };
})();