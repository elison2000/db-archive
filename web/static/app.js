/**
 * 数据库归档管理系统 - Alpine.js 声明式逻辑 (后端过滤与日期范围版)
 */

document.addEventListener('alpine:init', () => {
    Alpine.data('archiveApp', () => ({
        // 1. 响应式核心数据
        db: {
            datasource: [],
            job: [],
            task: [],
            detail: []
        },

        currentCategory: 'task',
        currentId: null,
        page: 1,
        size: 10,
        filterText: "",
        isRefreshing: false,

        // 新增过滤参数 (Job/Task) - 改为对象映射，保持状态独立
        filterParams: {
            datasource: {},
            job: {name: "", source_db: "", source_table: ""},
            task: {name: "", source_db: "", source_table: "", job_id: ""},
            detail: {}
        },

        // 日期范围过滤状态
        startDate: "",
        endDate: "",

        // 弹窗与表单状态
        showEditModal: false,
        showConfirmModal: false,
        formData: {},
        confirmTitle: '提示',
        confirmBody: '',
        confirmBtnColor: '',
        confirmAction: () => {
        },

        // 关联 ID 过滤状态
        filterState: {
            datasource: {key: 'id', value: null},
            job: {key: 'id', value: null},
            task: {key: 'job_id', value: null},
            detail: {key: 'task_id', value: null}
        },

        // 2. 初始化
        init() {
            // 1. 设置默认时间范围：7天前 ~ 今天
            const now = new Date();
            const startDay = new Date();
            startDay.setDate(now.getDate() - 7);

            const formatDate = (date) => date.toISOString().split('T')[0];

            // 先赋值默认值，此时 watch 还未建立，不会触发请求
            this.startDate = formatDate(startDay);
            this.endDate = formatDate(now);

            // 2. 建立监听：此时因为是 lazy 绑定，只有手动修改并确认后才会触发
            this.$watch('startDate', () => this.refreshData());
            this.$watch('endDate', () => this.refreshData());

            // 3. 执行初始加载
            this.refreshData();

            // 4. 暴露全局方法
            window.jumpToCategory = (cat, val) => {
                if (cat === 'task') {
                    // 当跳转到 task 页面时，自动填充 job_id 过滤
                    this.filterParams.task.job_id = val;
                    // 清除 filterState，避免双重过滤或冲突
                    this.filterState[cat].value = null; 
                    this.switchPage(cat);
                } else {
                    this.filterState[cat].value = val;
                    this.switchPage(cat);
                }
            };
            window.handleTaskControl = (type, id) => {
                this.handleTaskControl(type, id);
            };
            window.handleResumeTask = (id) => {
                this.handleResumeTask(id);
            };
        },

        // 3. 响应式方法 (前端仅负责模糊搜索和分页)
        filteredData() {
            let data = this.db[this.currentCategory] || [];
            // filterText 仅用于在已加载到本地的结果中进行二次检索
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

        // 4. 核心数据请求逻辑 (支持后端过滤参数)
        async refreshData() {
            const config = UI_CONFIG[this.currentCategory];
            const fs = this.filterState[this.currentCategory];
            let params = new URLSearchParams();

            if (fs && fs.value !== null) params.append(fs.key, fs.value);

            if (this.currentCategory === 'task' || this.currentCategory === 'detail') {
                if (this.startDate) params.append('start_date', this.startDate);
                if (this.endDate) params.append('end_date', this.endDate);
            }

            if (this.currentCategory === 'job' || this.currentCategory === 'task') {
                const fp = this.filterParams[this.currentCategory];
                if (fp.name) params.append('name', fp.name);
                if (fp.source_db) params.append('source_db', fp.source_db);
                if (fp.source_table) params.append('source_table', fp.source_table);
                if (this.currentCategory === 'task' && fp.job_id) params.append('job_id', fp.job_id);
            }

            const url = `${API_BASE}${config.endpoint}/?${params.toString()}`;

            try {
                const res = await fetch(url);
                if (res.ok) {
                    const json = await res.json();
                    this.db[this.currentCategory] = json;
                    DB_DATA[this.currentCategory] = json;
                    return true; // 新增：返回成功状态
                }
                throw new Error("Response not ok"); // 触发 catch
            } catch (e) {
                this.showToast("请求后端数据失败", "error");
                return false; // 新增：返回失败状态
            }
        },

        async refreshCurrentPage() {
            this.isRefreshing = true;
            const success = await this.refreshData(); // 获取执行结果

            setTimeout(() => {
                this.isRefreshing = false;
                // 只有在真正成功时才提示“刷新成功”
                if (success) {
                    this.showToast("查询成功");
                }
            }, 400);
        },

        switchPage(cat) {
            this.currentCategory = cat;
            this.page = 1;
            this.filterText = "";
            // 不再重置 filterParams，保持状态独立
            this.refreshData();
        },

        resetFilters() {
            if (this.filterParams[this.currentCategory]) {
                const fp = this.filterParams[this.currentCategory];
                Object.keys(fp).forEach(k => fp[k] = "");
            }
            // 同时重置模糊搜索和日期（如果是 task/detail）
            this.filterText = "";
            if (this.currentCategory === 'task' || this.currentCategory === 'detail') {
                // 恢复默认日期范围
                const now = new Date();
                const startDay = new Date();
                startDay.setDate(now.getDate() - 7);
                this.startDate = startDay.toISOString().split('T')[0];
                this.endDate = now.toISOString().split('T')[0];
            }
            this.refreshData();
        },

        clearLinkFilter() {
            this.filterState[this.currentCategory].value = null;
            this.refreshData();
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
                return `<div class="popover-wrapper"><span class="action-btn">查看配置</span><div class="popover-content"><pre>${pretty}</pre></div></div>`;
            }
            return col.isCode ? `<code>${val}</code>` : val;
        },

        // 5. 弹窗与表单
        openEditModal(id = null) {
            this.currentId = id;
            let item = id ? this.db[this.currentCategory].find(i => i.id === id) : {};
            if (!id) {
                item.is_enabled = 1; // 新增默认开启
                if (this.currentCategory === 'job' || this.currentCategory === 'task') {
                    Object.assign(item, {
                        split_size: 100000, batch_size: 1000, time_window: "23:00-06:00",
                        priority: 1, concurrency: 2, write_rate_limit: 10000, delete_rate_limit: 10000
                    });
                    if (this.currentCategory === 'job') item.interval_day = 1;
                    if (this.currentCategory === 'task') item.job_id = 0; // 默认单次任务
                }
            }
            this.formData = JSON.parse(JSON.stringify(item));
            this.showEditModal = true;
        },

        async testConnection() {
            try {
                const res = await fetch(`${API_BASE}/ping`, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(this.formData)
                });
                const data = await res.json();
                if (res.ok) {
                    this.showToast("连接成功");
                } else {
                    this.showToast(data.error || "连接失败", "error");
                }
            } catch (e) {
                this.showToast("连接服务器超时", "error");
            }
        },


        async testConfig() {
            try {


                // 克隆数据，避免污染原始表单
                let payload = JSON.parse(JSON.stringify(this.formData));
                if (this.currentCategory === 'task') {
                    delete payload.interval_day;
                }

                const res = await fetch(`${API_BASE}/test-archive-config`, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(payload) // 发送处理后的 payload
                });

                const data = await res.json();
                if (!res.ok) {
                    return this.showToast(data.error || "预检请求失败", "error");
                }

                // 处理后端返回的不同预检 code
                let type = 'success';
                if (data.code === 1) type = 'warning';
                if (data.code === 2 || data.code === -1) type = 'error';

                // 如果有详细错误 (detail)，在弹出消息中显示
                const displayMsg = data.detail ? `${data.msg}: ${data.detail}` : data.msg;
                this.showToast(displayMsg, type);

            } catch (e) {
                this.showToast("网络连接异常", "error");
            }
        },

        async handleSave() {
            const config = UI_CONFIG[this.currentCategory];
            for (let f of config.editFields) {
                if (f.required && (this.formData[f.name] === undefined || this.formData[f.name] === null || this.formData[f.name] === "")) {
                    return this.showToast(`请填写: ${f.label}`, "error");
                }
            }
            const isEdit = !!this.currentId;
            try {
                const res = await fetch(`${API_BASE}${config.endpoint}/${isEdit ? this.currentId : ''}`, {
                    method: isEdit ? 'PUT' : 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(this.formData)
                });
                if (res.ok) {
                    this.showToast("操作成功");
                    this.showEditModal = false;
                    this.refreshData();
                } else {
                    const err = await res.json();
                    this.showToast(err.error || "接口异常", "error");
                }
            } catch (e) {
                this.showToast("请求失败", "error");
            }
        },

        confirmDeleteModal(id) {
            this.confirmTitle = '删除确认';
            this.confirmBody = `确认要删除 ID 为 <b>${id}</b> 的记录吗？`;
            this.confirmBtnColor = 'var(--danger)';
            this.confirmAction = async () => {
                const res = await fetch(`${API_BASE}${UI_CONFIG[this.currentCategory].endpoint}/${id}`, {method: 'DELETE'});
                if (res.ok) {
                    this.showToast("删除成功");
                    this.showConfirmModal = false;
                    this.refreshData();
                }
            };
            this.showConfirmModal = true;
        },

        handleTaskControl(type, id) {
            const name = type === 'cancel' ? '暂停' : '终止';
            this.confirmTitle = `${name}确认`;
            this.confirmBody = `确认要<b>${name}</b> ID 为 <b>${id}</b> 的任务过程吗？`;
            this.confirmBtnColor = type === 'cancel' ? 'var(--warning)' : 'var(--danger)';
            this.confirmAction = async () => {
                const endpoint = type === 'cancel' ? 'cancel-archive-task' : 'terminate-archive-task';
                const res = await fetch(`${API_BASE}/${endpoint}/${id}`);
                const data = await res.json();
                if (res.ok) {
                    this.showToast(data.msg);
                    this.showConfirmModal = false;
                    this.refreshData();
                } else {
                    this.showToast(data.msg, "error");
                }
            };
            this.showConfirmModal = true;
        },


        async handleResumeTask(id) {
            try {
                const res = await fetch(`${API_BASE}/archive-tasks/${id}`, {
                    method: 'POST'
                });
                const data = await res.json();
                if (res.ok) {
                    this.showToast(data.msg);
                    this.refreshData(); // 刷新表格状态
                } else {
                    this.showToast(data.msg, "error");
                }
            } catch (e) {
                this.showToast("网络请求失败", "error");
            }
        },

        getDsDisplay(id) {
            const ds = this.db.datasource.find(d => d.id == id);
            return ds ? `[${ds.db_type}] ${ds.host}:${ds.port} (${ds.name})` : '';
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