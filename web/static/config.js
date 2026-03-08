const API_BASE = "/db-archive/api";

const DB_DATA = {
    datasource: [],
    job: [],
    task: [],
    detail: []
};

const UI_CONFIG = {
    // 1. 数据源页面配置 (完全冻结)
    datasource: {
        title: "数据源",
        modalWidth: "620px", // 保持原有的两列布局宽度
        endpoint: "/data-sources",
        columns: [
            {label: "ID", key: "id", width: "60px"},
            {label: "名称", key: "name"},
            {label: "角色", key: "role", isRoleTag: true},
            {label: "类型", key: "db_type", isTypeTag: true},
            {label: "地址", key: "host", format: (r) => `${r.host}:${r.port}`},
            {label: "用户名", key: "user"},
            {label: "扩展配置", key: "extra", isPopover: true},
            {label: "状态", key: "is_enabled", isStatus: true},
            {label: "备注", key: "remark"}
        ],
        editFields: [
            {name: 'name', label: '名称', type: 'text', required: true, col: 9},
            {name: 'is_enabled', label: '启用', type: 'checkbox', col: 3},
            {
                name: 'db_type',
                label: '数据库类型',
                type: 'select',
                options: ['mysql', 'doris', 'oracle'],
                required: true,
                col: 6
            },
            {name: 'role', label: "角色", type: "select", options: ["source", "sink", "both"], required: true, col: 6},
            {name: 'host', label: '主机地址', type: 'text', required: true, col: 8},
            {name: 'port', label: '端口', type: 'number', required: true, col: 4},
            {name: 'user', label: '用户名', type: 'text', required: true, col: 6},
            {name: 'password', label: '密码', type: 'text', required: true, col: 6},
            {name: 'remark', label: '备注', type: 'text', col: 12},
            {name: 'extra', label: '扩展配置(JSON)', type: 'textarea', col: 12}
        ],
        // 标记该模块需要测试连接功能
        showTestBtn: true
    },

    // 2. 周期配置页面配置 (优化为三列宽屏布局)
    job: {
        title: "周期调度",
        modalWidth: "1050px", // 针对 ArchiveJob 的多字段进行宽屏优化
        endpoint: "/archive-jobs",
        columns: [
            {label: "ID", key: "id", width: "50px"},
            {
                label: "源/目标详情",
                key: "source_target",
                format: (r) => {
                    const sInfo = DB_DATA.datasource.find(d => d.id === r.source_id) || {};
                    const kInfo = DB_DATA.datasource.find(d => d.id === r.sink_id) || {};
                    const sPop = sInfo.id ? `类型:${sInfo.db_type}\n地址:${sInfo.host}:${sInfo.port}`.replace(/"/g, '&quot;') : '未找到';
                    const kPop = kInfo.id ? `类型:${kInfo.db_type}\n地址:${kInfo.host}:${kInfo.port}`.replace(/"/g, '&quot;') : '未找到';

                    return `
                    <div style="display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: #606266;">
                        <div style="display: flex; align-items: center; gap: 6px;">
                            <span style="width: 45px;">源:</span>
                            <div class="popover-wrapper">
                                <span class="tag primary" style="cursor:help; padding:1px 4px;">ID=${r.source_id}</span>
                                <div class="popover-content"><pre>${sPop}</pre></div>
                            </div>
                            <span>${r.source_db}.${r.source_table}</span>
                        </div>
                        <div style="display: flex; align-items: center; gap: 6px;">
                            <span style="width: 45px;">目标:</span>
                            <div class="popover-wrapper">
                                <span class="tag primary" style="cursor:help; padding:1px 4px;">ID=${r.sink_id}</span>
                                <div class="popover-content"><pre>${kPop}</pre></div>
                            </div>
                            <span>${r.sink_db}.${r.sink_table}</span>
                        </div>
                    </div>`;
                }
            },
            {
                label: "归档名称与配置",
                key: "name_cfg",
                width: "220px",
                format: (r) => {
                    const mColor = {
                        'move': 'warning',
                        'copy_only': 'primary',
                        'delete_only': 'danger'
                    }[r.archive_mode] || 'info';
                    const wColor = r.write_mode === 'upsert' ? 'warning' : 'success';
                    // 增加 HTML 转义逻辑防止 < 符号导致渲染异常
                    const rawCond = (r.archive_condition || '').replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
                    const isLong = rawCond.length > 30;
                    const displayCond = isLong ? rawCond.substring(0, 30) + '...' : rawCond;
                    const condHtml = isLong ? `
                       <div class="popover-wrapper">
                        <code style="cursor:help; color:var(--primary); font-weight:500;">${displayCond}</code>
                        <div class="popover-content"><pre>${rawCond}</pre></div>
                    </div>` : `<code>${displayCond}</code>`;

                    return `
                    <div style="line-height:1.6">
                        <b style="color:#606266">${r.name}</b><br/>
                        <span class="tag ${mColor}">${r.archive_mode}</span>
                        <span class="tag ${wColor}" style="margin-left:4px;">${r.write_mode}</span>
                        <div style="font-size:11px; color:#606266; margin-top:2px;">条件: ${condHtml}</div>
                    </div>`;
                }
            },
            {
                label: "分批配置",
                key: "split_cfg",
                format: (r) => `
            <div style="font-size:12px; line-height: 1.8; color: #606266;">
                切分字段: <span style="color: #303133;">${r.split_column || '-'}</span><br/>
                切分大小: <span style="color:#303133;">${r.split_size}</span><br/>
                单批行数: <span style="color:#303133;">${r.batch_size}</span>
            </div>`
            },
            {
                label: "调度/优先级",
                key: "schedule",
                format: (r) => `
            <div style="font-size:12px; line-height: 1.8; color: #606266;">
                周期: <span style="color: #303133;">${r.interval_day}天</span><br/>
                窗口: <span style="color: #303133;">${r.time_window}</span><br/>
                权重: <span style="color: #303133;">${r.priority}</span>
            </div>`
            },
            {
                label: "并发与限速",
                key: "performance",
                format: (r) => `
            <div style="font-size:12px; line-height: 1.8; color: #606266;">
                并发: <span style="color: #303133;">${r.concurrency}</span><br/>
                写速: <span style="color: #303133;">${r.write_rate_limit}</span><br/>
                删速: <span style="color: #303133;">${r.delete_rate_limit}</span>
            </div>`
            },

            {
                label: "任务", key: "detail_btn", format: (r) => `
            <span class="action-btn" onclick="jumpToCategory('task', ${r.id})">查看</span>`
            },
            {label: "状态", key: "is_enabled", isStatus: true},
            {label: "备注", key: "remark"}
        ],


        editFields: [
            // 第一排
            {name: 'name', label: '任务名称', type: 'text', required: true, col: 6},
            {name: 'is_enabled', label: '启用', type: 'checkbox', col: 3},

            // 源配置组
            {
                name: 'source_id',
                label: '源数据源ID',
                type: 'datasourceSelect',
                roleFilter: ['source', 'both'],
                required: true,
                col: 4
            },
            {name: 'source_db', label: '源数据库', type: 'text', required: true, col: 4},
            {name: 'source_table', label: '源表名', type: 'text', required: true, col: 4},

            // 目标配置组
            {
                name: 'sink_id',
                label: '目标数据源ID',
                type: 'datasourceSelect',
                roleFilter: ['sink', 'both'],
                required: true,
                col: 4
            },
            {name: 'sink_db', label: '目标数据库', type: 'text', required: true, col: 4},
            {name: 'sink_table', label: '目标表名', type: 'text', required: true, col: 4},

            // 模式与条件
            {
                name: 'archive_mode',
                label: '归档模式',
                type: 'select',
                options: ['move', 'copy_only', 'delete_only'],
                required: true,
                col: 4
            },
            {
                name: 'write_mode',
                label: '写入模式',
                type: 'select',
                options: ['insert', 'upsert'],
                required: true,
                col: 4
            },
            {name: 'archive_condition', label: '归档条件(WHERE)', type: 'text', required: true, col: 4},


            {name: 'split_column', label: '切分字段', type: 'text', col: 4},
            {name: 'split_size', label: '切分大小', type: 'number', col: 4},
            {name: 'batch_size', label: '单批行数(控制事务大小)', type: 'number', col: 4},

            // 调度与性能
            {name: 'interval_day', label: '间隔天数', type: 'number', required: true, col: 4},
            {name: 'time_window', label: '执行窗口', type: 'text', required: true, col: 4},
            {name: 'priority', label: '优先级(1最高)', type: 'number', required: true, col: 4},


            {name: 'concurrency', label: '并发数', type: 'number', col: 4},
            {name: 'write_rate_limit', label: '写入限速', type: 'number', col: 4},
            {name: 'delete_rate_limit', label: '删除限速', type: 'number', col: 4},


            // 备注
            {name: 'remark', label: '备注', type: 'text', col: 12}
        ]
    },


    // task 配置
    task: {
        title: "归档任务",
        modalWidth: "1050px", // 任务记录包含更多统计信息，调大宽度
        endpoint: "/archive-tasks",
        columns: [
            {label: "ID", key: "id", width: "50px"},

            {
                label: "源/目标详情",
                key: "source_target",
                format: (r) => {
                    const sInfo = DB_DATA.datasource.find(d => d.id === r.source_id) || {};
                    const kInfo = DB_DATA.datasource.find(d => d.id === r.sink_id) || {};
                    const sPop = sInfo.id ? `类型:${sInfo.db_type}\n地址:${sInfo.host}:${sInfo.port}`.replace(/"/g, '&quot;') : '未找到';
                    const kPop = kInfo.id ? `类型:${kInfo.db_type}\n地址:${kInfo.host}:${kInfo.port}`.replace(/"/g, '&quot;') : '未找到';

                    return `
                    <div style="display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: #606266;">
                        <div style="display: flex; align-items: center; gap: 6px;">
                            <span style="width: 45px;">源:</span>
                            <div class="popover-wrapper">
                                <span class="tag primary" style="cursor:help; padding:1px 4px;">ID=${r.source_id}</span>
                                <div class="popover-content"><pre>${sPop}</pre></div>
                            </div>
                            <span>${r.source_db}.${r.source_table}</span>
                        </div>
                        <div style="display: flex; align-items: center; gap: 6px;">
                            <span style="width: 45px;">目标:</span>
                            <div class="popover-wrapper">
                                <span class="tag primary" style="cursor:help; padding:1px 4px;">ID=${r.sink_id}</span>
                                <div class="popover-content"><pre>${kPop}</pre></div>
                            </div>
                            <span>${r.sink_db}.${r.sink_table}</span>
                        </div>
                    </div>`;
                }
            },
            {
                label: "归档名称与配置",
                key: "name_cfg",
                width: "220px",
                format: (r) => {
                    const mColor = {
                        'move': 'warning',
                        'copy_only': 'primary',
                        'delete_only': 'danger'
                    }[r.archive_mode] || 'info';
                    const wColor = r.write_mode === 'upsert' ? 'warning' : 'success';
                    // 增加 HTML 转义逻辑防止 < 符号导致渲染异常
                    const rawCond = (r.archive_condition || '').replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
                    const isLong = rawCond.length > 30;
                    const displayCond = isLong ? rawCond.substring(0, 30) + '...' : rawCond;
                    const condHtml = isLong ? `
                    <div class="popover-wrapper">
                        <code style="cursor:help; color:var(--primary); font-weight:500;">${displayCond}</code>
                        <div class="popover-content"><pre>${rawCond}</pre></div>
                    </div>` : `<code>${displayCond}</code>`;

                    return `
                    <div style="line-height:1.6">
                        <b style="color:#606266">${r.name}</b><br/>
                        <span class="tag ${mColor}">${r.archive_mode}</span>
                        <span class="tag ${wColor}" style="margin-left:4px;">${r.write_mode}</span>
                        <div style="font-size:11px; color:#606266; margin-top:2px;">条件: ${condHtml}</div>
                    </div>`;
                }
            },
            {
                label: "分批配置",
                key: "split_cfg",
                format: (r) => `
            <div style="font-size:12px; line-height: 1.8; color: #606266;">
                切分字段: <span style="color: #303133;">${r.split_column || '-'}</span><br/>
                切分大小: <span style="color:#303133;">${r.split_size}</span><br/>
                单批行数: <span style="color:#303133;">${r.batch_size}</span>
            </div>`
            },
            {
                label: "调度/优先级",
                key: "schedule",
                format: (r) => {
                    const type = r.job_id === 0 ? '单次' : '周期';
                    return `
                <div style="font-size:12px; line-height: 1.8; color: #606266;">
                类型: <span class="tag primary">${type}</span><br/>
                窗口: <span style="color: #303133;">${r.time_window}</span><br/>
                权重: <span style="color: #303133;">${r.priority}</span>
                </div>`;
                }
            },
            {
                label: "并发与限速",
                key: "performance",
                format: (r) => `
            <div style="font-size:12px; line-height: 1.8; color: #606266;">
                并发: <span style="color: #303133;">${r.concurrency}</span><br/>
                写速: <span style="color: #303133;">${r.write_rate_limit}</span><br/>
                删速: <span style="color: #303133;">${r.delete_rate_limit}</span>
            </div>`
            },

            {
                label: "阶段/状态",
                key: "phase_status",
                format: (r) => {
                    const execColors = {
                        'init': 'info', 'preparing': 'primary', 'prepared': 'success',
                        'completed': 'success', 'failed': 'danger', 'queueing': 'primary',
                        'running': 'primary', 'paused': 'warning', 'stopped': 'warning'
                    };
                    const pColor = execColors[r.prepare_phase] || 'info';
                    const eColor = execColors[r.exec_phase] || 'info';

                    // 动态按钮逻辑
                    let actionHtml = '';
                    if (r.exec_phase === 'running') {
                        actionHtml = `
             <div style="margin-top: 8px; display: flex; gap: 6px;">
                <span class="tag warning" style="cursor:pointer; padding:3px 10px; border-radius:2px; font-weight:bold; font-size:11px; box-shadow:0 2px 4px rgba(230,162,60,0.3)" onclick="handleTaskControl('cancel', ${r.id})">暂停</span>
                <span class="tag warning" style="cursor:pointer; padding:3px 10px; border-radius:2px; font-weight:bold; font-size:11px; box-shadow:0 2px 4px rgba(230,162,60,0.3)" onclick="handleTaskControl('terminate', ${r.id})">终止</span>
            </div>`;
                    } else if (r.exec_phase === 'stopped') {
                        // 增加恢复执行按钮
                        actionHtml = `
            <div style="margin-top: 8px; display: flex; gap: 6px;">
                <span class="tag warning" style="cursor:pointer; padding:3px 10px; border-radius:2px; font-weight:bold; font-size:11px; box-shadow:0 2px 4px rgba(230,162,60,0.3)" onclick="handleResumeTask(${r.id})">恢复执行</span>
            </div>`;
                    }

                    return `<div style="line-height:1.6">准备: <span class="tag ${pColor}">${r.prepare_phase}</span><br/>执行: <span class="tag ${eColor}">${r.exec_phase}</span>${actionHtml}</div>`;
                }
            },


            {
                label: "行数统计",
                key: "rows_stat",
                format: (r) => `
                <div style="font-size:12px; line-height:1.6">
                    读取: <span style="color:#303133">${r.read_rows}</span><br/>
                    写入: <span style="color:#303133">${r.inserted_rows}</span><br/>
                    删除: <span style="color:#303133">${r.deleted_rows}</span>
                </div>`
            },
            {
                label: "时间节点",
                key: "time_nodes",
                width: "180px",
                format: (r) => `
        <div style="font-size:12px; line-height:1.6; color:#606266;">
            创建: <span>${r.created_at ? r.created_at.replace('T', ' ').substring(0, 19) : '-'}</span><br/>
            开始: <span>${r.exec_start ? r.exec_start.replace('T', ' ').substring(0, 19) : '-'}</span><br/>
            结束: <span>${r.exec_end ? r.exec_end.replace('T', ' ').substring(0, 19) : '-'}</span>
        </div>`
            },
            {
                label: "执行详情",
                key: "combined_info",
                format: (r) => {
                    const msg = r.msg || '-';
                    let msgHtml = '消息: -';
                    if (msg && msg !== '-') {
                        const isLong = msg.length > 10;
                        const displayMsg = isLong ? msg.substring(0, 10) + '...' : msg;
                        msgHtml = `<div class="popover-wrapper">
                消息: <span style="color:${r.exec_phase === 'failed' ? '#F56C6C' : '#909399'}; cursor:help; text-decoration:underline;">${displayMsg}</span>
                <div class="popover-content"><pre>${msg}</pre></div>
            </div>`;
                    }
                    return `
            <div style="font-size:12px; line-height:1.8">
                耗时: <span>${r.exec_seconds}s</span><br/>
                ${msgHtml}
                <div style="margin-top:2px;">
                    <span class="action-btn" onclick="window.open('/db-archive/sub-task/${r.id}')">查看子任务</span>
                </div>
            </div>`;
                }
            },
            {label: "状态", key: "is_enabled", isStatus: true}
        ],
        editFields: [
            {name: 'name', label: '任务名称', type: 'text', required: true, col: 6},
            {name: 'job_id', label: '关联Job ID', type: 'number', col: 3},
            {name: 'is_enabled', label: '启用', type: 'checkbox', col: 3},


            {
                name: 'source_id',
                label: '源数据源ID',
                type: 'datasourceSelect',
                roleFilter: ['source', 'both'],
                required: true,
                col: 4
            },
            {name: 'source_db', label: '源数据库', type: 'text', required: true, col: 4},
            {name: 'source_table', label: '源表名', type: 'text', required: true, col: 4},

            {
                name: 'sink_id',
                label: '目标数据源ID',
                type: 'datasourceSelect',
                roleFilter: ['sink', 'both'],
                required: true,
                col: 4
            },
            {name: 'sink_db', label: '目标数据库', type: 'text', required: true, col: 4},
            {name: 'sink_table', label: '目标表名', type: 'text', required: true, col: 4},

            {
                name: 'archive_mode',
                label: '归档模式',
                type: 'select',
                options: ['move', 'copy_only', 'delete_only'],
                required: true,
                col: 4
            },
            {
                name: 'write_mode',
                label: '写入模式',
                type: 'select',
                options: ['insert', 'upsert'],
                required: true,
                col: 4
            },
            {name: 'archive_condition', label: '归档条件(WHERE)', type: 'text', required: true, col: 4},

            {name: 'split_column', label: '切分字段', type: 'text', col: 4},
            {name: 'split_size', label: '切分大小', type: 'number', col: 4},
            {name: 'batch_size', label: '单批行数(控制事务大小)', type: 'number', col: 4},

            // 调度与性能
            {name: 'time_window', label: '执行窗口', type: 'text', required: true, col: 4},
            {name: 'priority', label: '优先级(1最高)', type: 'number', required: true, col: 4},

            {name: 'concurrency', label: '并发数', type: 'number', col: 4},
            {name: 'write_rate_limit', label: '写入限速', type: 'number', col: 4},
            {name: 'delete_rate_limit', label: '删除限速', type: 'number', col: 4}
        ]
    },


    detail: {
        title: "子任务",
        endpoint: "/archive-sub-tasks",
        columns: [
            {label: "ID", key: "id", width: "50px"},
            {label: "关联任务", key: "task_id", width: "80px", format: (r) => `${r.task_id}`},
            {
                label: "分片范围/条件",
                key: "split_info",
                width: "300px",
                format: (r) => {
                    // 增加 HTML 转义逻辑防止 < 符号导致渲染异常
                    const fullCond = (r.full_condition || '').replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
                    const isLong = fullCond.length > 40;
                    const displayCond = isLong ? fullCond.substring(0, 40) + '...' : fullCond;

                    // 完整条件支持气泡显示
                    const condHtml = `
                    <div class="popover-wrapper">
                        <code style="cursor:help; color:var(--primary); font-weight:500;">${displayCond}</code>
                        <div class="popover-content"><pre>${fullCond}</pre></div>
                    </div>`;

                    return `
                    <div style="line-height:1.6">
                        <div style="font-size:12px; color:#303133">
                            字段: <span>${r.split_column}</span><br/>
                            范围: <span>${r.start_value}</span> ➔ <span>${r.end_value}</span>
                        </div>
                        <div style="font-size:11px; color:#606266; margin-top:2px;">条件: ${condHtml}</div>
                    </div>`;
                }
            },
            {
                label: "执行状态",
                key: "exec_phase",
                format: (r) => {
                    const execColors = {
                        'completed': 'success',
                        'failed': 'danger',
                        'running': 'primary',
                        'paused': 'warning',
                        'pending': 'info'
                    };
                    const eColor = execColors[r.exec_phase] || 'info';
                    return `<span class="tag ${eColor}">${r.exec_phase}</span>`;
                }
            },
            {
                label: "行列统计",
                key: "rows_stat",
                format: (r) => `
                <div style="font-size:12px; line-height:1.6">
                    读取: <span style="color:#303133">${r.read_rows}</span><br/>
                    写入: <span style="color:#303133">${r.inserted_rows}</span><br/>
                    删除: <span style="color:#303133">${r.deleted_rows}</span>
                </div>`
            },


            {
                label: "时间节点",
                key: "time_nodes",
                width: "180px",
                format: (r) => `
        <div style="font-size:12px; line-height:1.6; color:#606266;">
            创建: <span>${r.created_at ? r.created_at.replace('T', ' ').substring(0, 19) : '-'}</span><br/>
            开始: <span>${r.exec_start ? r.exec_start.replace('T', ' ').substring(0, 19) : '-'}</span><br/>
            结束: <span>${r.exec_end ? r.exec_end.replace('T', ' ').substring(0, 19) : '-'}</span>
        </div>`
            },
            {
                label: "执行详情",
                key: "combined_info",
                format: (r) => {
                    const msg = r.msg || '-';
                    let msgHtml = '消息: -';
                    if (msg && msg !== '-') {
                        const isLong = msg.length > 10;
                        const displayMsg = isLong ? msg.substring(0, 10) + '...' : msg;
                        msgHtml = `<div class="popover-wrapper">
                消息: <span style="color:${r.exec_phase === 'failed' ? '#F56C6C' : '#909399'}; cursor:help; text-decoration:underline;">${displayMsg}</span>
                <div class="popover-content"><pre>${msg}</pre></div>
            </div>`;
                    }
                    return `
            <div style="font-size:12px; line-height:1.8">
                耗时: <span>${r.exec_seconds}s</span><br/>
                ${msgHtml}
            </div>`;
                }
            }
        ],
        // 此模块不需要编辑和新增功能
        editFields: []
    }
};