/**
 * 子任务详情页面逻辑
 */

document.addEventListener('alpine:init', () => {
    Alpine.data('subTaskApp', () => ({
        taskId: null,
        dataList: [],
        page: 1,
        size: 10,
        filterText: "",
        execPhase: "",
        isRefreshing: false,
        columns: UI_CONFIG.detail.columns,

        init() {
            // 从 URL 获取 taskId
            // 假设 URL 格式为 /db-archive/sub-task/:id
            const pathParts = window.location.pathname.split('/');
            this.taskId = pathParts[pathParts.length - 1];

            if (this.taskId) {
                this.refreshData();
            } else {
                this.showToast("未找到任务ID", "error");
            }
        },

        filteredData() {
            let data = this.dataList || [];
            
            // 1. exec_phase 前端过滤
            if (this.execPhase) {
                data = data.filter(r => r.exec_phase === this.execPhase);
            }

            // 2. 模糊搜索 (filterText)
            if (this.filterText) {
                const s = this.filterText.toLowerCase();
                data = data.filter(r => JSON.stringify(r).toLowerCase().includes(s));
            }
            return data;
        },

        pagedData() {
            const start = (this.page - 1) * this.size;
            return this.filteredData().slice(start, start + this.size);
        },

        totalItems() {
            return this.filteredData().length;
        },

        totalPages() {
            return Math.ceil(this.totalItems() / this.size) || 1;
        },

        async refreshData() {
            const endpoint = UI_CONFIG.detail.endpoint;
            // 构造查询参数
            let params = new URLSearchParams();
            params.append('task_id', this.taskId);

            const url = `${API_BASE}${endpoint}/?${params.toString()}`;

            try {
                const res = await fetch(url);
                if (res.ok) {
                    const json = await res.json();
                    this.dataList = json;
                    return true;
                }
                throw new Error("Response not ok");
            } catch (e) {
                this.showToast("请求数据失败", "error");
                return false;
            }
        },

        async refreshCurrentPage() {
            this.isRefreshing = true;
            const success = await this.refreshData();
            setTimeout(() => {
                this.isRefreshing = false;
                if (success) {
                    this.showToast("查询成功");
                }
            }, 400);
        },

        formatCell(row, col) {
            const val = col.format ? col.format(row) : row[col.key];
            if (val === null || val === undefined) return '-';

            if (col.isRoleTag) {
                const colors = {'both': 'danger', 'source': 'warning', 'sink': 'success'};
                return `<span class="tag ${colors[val] || 'info'}">${val}</span>`;
            }
            if (col.isTypeTag) {
                const colors = {'mysql': 'primary', 'oracle': 'warning', 'pgsql': 'success', 'doris': 'danger'};
                return `<span class="tag ${colors[val.toLowerCase()] || 'primary'}">${val}</span>`;
            }
            if (col.isStatus) return `<span class="tag ${val === 1 ? 'success' : 'info'}">${val === 1 ? '启用' : '禁用'}</span>`;
            if (col.isPopover && val) {
                const pretty = JSON.stringify(typeof val === 'string' ? JSON.parse(val) : val, null, 2).replace(/"/g, '&quot;');
                return `<div class="popover-wrapper" onclick="this.classList.toggle('active')"><span class="action-btn">查看配置</span><div class="popover-content"><pre>${pretty}</pre></div></div>`;
            }
            return col.isCode ? `<code>${val}</code>` : val;
        },

        showToast(msg, type = 'success') {
            const c = document.querySelector('.toast-container') || document.body.appendChild(Object.assign(document.createElement('div'), {className: 'toast-container'}));
            const t = Object.assign(document.createElement('div'), {className: `toast ${type}`, innerText: msg});
            c.appendChild(t);
            setTimeout(() => {
                t.classList.add('fade-out');
                setTimeout(() => t.remove(), 300);
            }, 3000);
        }
    }));
});
