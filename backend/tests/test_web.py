"""The small web shell must not acquire work or depend on DB availability."""
from uuid import uuid4
from html.parser import HTMLParser
from pathlib import Path
import re
import shutil
import subprocess

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.config import Settings


def test_monitor_controls_exist_once_in_the_served_shell():
    class Controls(HTMLParser):
        def __init__(self):
            super().__init__()
            self.ids = []
            self.forms = []
            self.input_types = []

        def handle_starttag(self, tag, attributes):
            self.ids.extend(value for name, value in attributes if name == "id")
            attrs = dict(attributes)
            if tag == "form":
                self.forms.append(attrs.get("id"))
            if tag == "input":
                self.input_types.append(attrs.get("type"))

    web = Path(__file__).parents[1] / "app" / "web"
    parser = Controls()
    parser.feed((web / "index.html").read_text(encoding="utf-8"))
    required = set(re.findall(r'\$\("([^"\n]+)"\)', (web / "app.js").read_text(encoding="utf-8")))
    assert not required - set(parser.ids), f"Missing script controls: {required - set(parser.ids)}"
    assert len(parser.ids) == len(set(parser.ids)), "Duplicate DOM IDs break monitor controls"
    assert parser.forms == ["queue-search-form"]
    assert "file" not in parser.input_types
    assert {
        "queue-list", "queue-search", "queue-status", "steps", "step-inspector",
        "attempts", "events", "result", "artifacts", "cancel-button", "resume-button",
        "node-resources", "nodes-status", "resource-warning", "resource-attention",
    } <= set(parser.ids)
    assert not {"legacy-cpu", "cpu-resource", "resource-status", "resource"} & set(parser.ids)
    assert "旧兼容执行域" not in (web / "index.html").read_text(encoding="utf-8")


def test_monitor_assets_remove_creation_without_reintroducing_its_session_state():
    web = Path(__file__).parents[1] / "app" / "web"
    script = (web / "app.js").read_text(encoding="utf-8")
    assets = "\n".join((web / name).read_text(encoding="utf-8") for name in ("index.html", "app.js", "app.css"))
    for selector in (
        "new-task-button", "close-create", "create-panel", "create-form", "create-button",
        "image-input", "pipeline-select", "seed-input", "generation-tag",
        "cache-enabled", "bypass-cache", "upload-controls", "cache-options", "checkbox-label",
    ):
        assert selector not in assets
    assert '"/api/v1/inputs"' not in script
    assert 'readPending("upload"' not in script
    assert 'savePending("upload"' not in script
    assert "上传图片创建" not in assets
    assert "历史三步" in script


def test_managed_monitor_distinguishes_open_registration_from_completion():
    web = Path(__file__).parents[1] / "app" / "web"
    script = (web / "app.js").read_text(encoding="utf-8")
    assert 'task.pipeline_id === "managed-v1" && task.registration_closed === false' in script
    assert 'if (!isOpenManaged(task)) {' in script
    assert 'WAITING_STEPS: "等待追加工作流步骤"' in script
    assert 'WAITING_CLEANUP: "等待退出确认"' in script
    assert '已登记 ${registered} 步 / 已完成 ${completed} 步' in script
    assert 'if (task.registration_closed) return "登记已结束";' in script
    assert '"流程未结束"' in script
    assert 'a.registration_order - b.registration_order' in script
    assert 'task.pipeline_id === "managed-v1" ? managedNames : names' in script
    assert 'task.status, task.registration_closed, task.output_refs, artifacts' in script


def test_node_resource_renderer_keeps_device_owners_and_drain_independent():
    node = shutil.which("node")
    if not node:
        pytest.skip("The no-browser DOM renderer test requires Node.js")
    web = Path(__file__).parents[1] / "app" / "web"
    script = r'''
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const elements = new Map();
let document;
class Element {
  constructor(tag = 'div') {
    this.tagName = tag; this.children = []; this.dataset = {}; this.attributes = {}; this.style = {}; this._text = '';
  }
  set textContent(value) { this._text = String(value); this.children = []; }
  get textContent() { return this._text + this.children.map(child => child.textContent).join(''); }
  append(...children) { for (const child of children) { child.parent = this; this.children.push(child); } }
  replaceChildren(...children) { this._text = ''; this.children = []; this.append(...children); }
  setAttribute(name, value) { this.attributes[name] = String(value); }
  addEventListener(name, handler) { (this.listeners ||= {})[name] = handler; }
  matches(selector) {
    const tag = selector.match(/^[a-z]+/i)?.[0];
    if (tag && this.tagName !== tag) return false;
    for (const [, name] of selector.matchAll(/\.([\w-]+)/g)) if (!(this.className || '').split(' ').includes(name)) return false;
    for (const [, name, value] of selector.matchAll(/\[([^=\]]+)(?:="([^"]*)")?\]/g)) {
      const actual = name === 'open' ? (this.open ? '' : undefined) : name.startsWith('data-') ? this.dataset[name.slice(5).replace(/-([a-z])/g, (_, c) => c.toUpperCase())] : this.attributes[name];
      if (actual === undefined || (value !== undefined && String(actual) !== value)) return false;
    }
    return true;
  }
  querySelectorAll(selector) {
    const [first, ...tail] = selector.split(' '), result = [];
    for (const child of this.children) {
      if (child.matches(first)) result.push(...(tail.length ? child.querySelectorAll(tail.join(' ')) : [child]));
      result.push(...child.querySelectorAll(selector));
    }
    return result;
  }
  querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
  closest(selector) { return this.matches(selector) ? this : this.parent?.closest(selector) || null; }
  focus() { document.activeElement = this; }
  scrollIntoView() { this.scrolled = true; }
}
const get = id => { if (!elements.has(id)) elements.set(id, new Element()); return elements.get(id); };
document = {
  getElementById: get, createElement: tag => new Element(tag), activeElement: null,
  querySelectorAll: selector => [...elements.values()].flatMap(root => root.querySelectorAll(selector)),
};
const context = {
  document, URLSearchParams, CSS: {escape: value => value}, requestAnimationFrame: () => {},
  sessionStorage: {getItem: () => null}, fetch: () => { throw new Error('Rendering must not call APIs or executors'); },
};
const source = fs.readFileSync('app.js', 'utf8');
const boundary = source.indexOf('  $("resume-button").addEventListener("click", async () => {');
assert.ok(boundary > 0);
vm.runInNewContext(source.slice(0, boundary) + `
  globalThis.view = {
    renderResource, renderQueue, renderAttention, renderTaskBrief, renderArtifacts, stepName, stepModel,
    eventList(task, items) { events = new Map(items.map(item => [item.seq, item])); renderEvents(task); },
    task(task, attempts) {
      taskId = task.id; snapshot = task; history = attempts; render(task, []);
    },
    inspect(key) { activeStep = key; followStep = false; renderStepInspector(snapshot); },
    follow(target) { detailTarget = target; revealExecution(); },
    tab() { return detailTab; }
  };
})();`, context);
const view = context.view;
const taskA = '11111111-1111-4111-8111-111111111111', taskB = '22222222-2222-4222-8222-222222222222';
const attemptA = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', attemptB = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb';
const legacy = {cpu: {capacity: 2, in_use: 1}, slots: [{id: 1, state: 'AVAILABLE'}]};
const resources = {...legacy, nodes: [
  {node_id: 'node-a', executor_id: 'executor-a', origin: 'https://node-a.test', accepting: false, configured: true, status: 'ONLINE', storage_id: 'shared',
    last_seen_at: '2026-10-25T00:00:00Z', last_probe_at: '2026-10-25T00:00:00Z', last_error: null,
    capabilities: {processors: {mock: ['1', '2']}, input_kinds: ['synthetic', 'uploaded'], step_keys: ['normalize_asset', 'constructor'],
      named_step_processors: ['mock-step'], capabilities: ['renewable-lease', 'future-protocol'], references_readable: true,
      protocol_version: 1, artifact_schema_versions: [1], state_id: 'state-a', cpu_capacity: 3, cpu_in_use: 2,
      mock_gpu_slots: [{device_id: 'gpu-0', state: 'RUNNING'}]}, cpu: {capacity: 3, in_use: 2},
    slots: [{id: 101, node_id: 'node-a', device_id: 'gpu-0', state: 'QUARANTINED', owner_attempt_id: attemptA, owner_generation: 3, task_id: taskA, step_key: 'constructor'}],
    events: [{type: 'NODE_ACCEPTING_CHANGED', created_at: '2026-10-25T00:00:00Z', payload: {accepting: false}},
      {type: 'NODE_CONFIGURED', payload: {cpu_capacity: 3, devices: ['gpu-0', 'gpu-1']}},
      {type: 'NODE_UNREACHABLE', payload: {error: 'TLS_ERROR'}},
      {type: 'FUTURE_EVENT', payload: {message: '<img src=x onerror=alert(1)>', extra: {code: 'UNKNOWN_CODE'}}}]},
  {node_id: 'node-b', executor_id: 'executor-b', origin: 'https://node-b.test', accepting: true, configured: true, status: 'OFFLINE', storage_id: 'shared',
    last_seen_at: null, last_probe_at: '2026-10-25T00:00:00Z', last_error: 'EXECUTOR_UNAVAILABLE', capabilities: null, cpu: {capacity: 4, in_use: 1},
    slots: [{id: 102, node_id: 'node-b', device_id: 'gpu-0', state: 'RUNNING', owner_attempt_id: attemptB, owner_generation: 7, task_id: taskB, step_key: 'work-b'}],
    events: [{type: 'NODE_OFFLINE', created_at: '2026-10-25T00:00:00Z', payload: {status: 'OFFLINE'}}]},
]};
view.renderResource(resources);
const nodes = get('node-resources').children;
assert.equal(nodes.length, 2);
assert.equal(get('nodes-status').textContent, '1/2 在线');
assert.equal(elements.has('cpu-resource'), false); // legacy resources remain API-only
assert.match(nodes[0].textContent, /CPU 任务并发 2\/3 · 当前占用 \/ 并发上限/);
assert.match(nodes[1].textContent, /CPU 任务并发 1\/4 · 当前占用 \/ 并发上限/);
assert.equal(nodes[0].querySelector('.status').textContent, '在线');
assert.equal(nodes[0].querySelector('.node-accepting').textContent, '停止接单');
assert.equal(nodes[1].querySelector('.status').textContent, '离线');
assert.equal(nodes[1].querySelector('.node-accepting').textContent, '接单中');
const slots = nodes.map(node => node.querySelector('.resource-state'));
assert.deepEqual(slots.map(slot => [slot.dataset.node, slot.dataset.device]), [['node-a', 'gpu-0'], ['node-b', 'gpu-0']]);
assert.match(slots[0].textContent, /GPU 执行槽位gpu-0.*已隔离/);
assert.match(slots[1].textContent, /GPU 执行槽位gpu-0.*运行中/);
assert.match(slots[0].textContent, /等待原执行退出确认/);
for (const [index, task, step, attempt, generation] of [[0, taskA, 'constructor', attemptA, 3], [1, taskB, 'work-b', attemptB, 7]]) {
  const links = slots[index].querySelectorAll('a');
  assert.equal(links.length, 2);
  assert.equal(links[0].href, `/tasks/${task}?step=${step}`);
  assert.equal(links[1].href, `/tasks/${task}?step=${step}&attempt=${attempt}`);
  assert.ok(links[1].title.includes(attempt));
  assert.ok(slots[index].textContent.includes(`代次 ${generation}`));
}
assert.ok(nodes[0].textContent.includes('NODE_ACCEPTING_CHANGED'));
assert.ok(nodes[1].textContent.includes('NODE_OFFLINE'));
assert.ok(nodes[0].textContent.includes('https://node-a.test'));
const info = nodes[0].querySelector('[data-detail="node-info:node-a"]'); info.open = true;
assert.equal(info.querySelectorAll('pre').length, 0);
assert.match(info.textContent, /执行能力.*任务处理器.*内置步骤处理器 · v1 \/ v2/);
assert.match(info.textContent, /输入来源.*合成测试数据.*用户上传素材/);
assert.match(info.textContent, /输入素材预处理.*constructor/);
assert.match(info.textContent, /租约续期.*future-protocol/);
assert.match(info.textContent, /共享存储可读性检查：通过/);
assert.match(info.textContent, /执行状态库 IDstate-a/);
assert.match(info.textContent, /可靠性与安全.*运行时信息.*执行协议版本1.*资产元数据版本1/);
assert.equal(info.querySelectorAll('.capability-list small').length, 0);
const technical = info.querySelector('.capability-technical');
assert.match(technical.textContent, /CPU 任务并发2 \/ 3节点报告 · 当前占用 \/ 并发上限/);
assert.match(technical.textContent, /处理器标识mock@1\/2.*输入来源标识synthetic、uploaded.*工作流步骤标识normalize_asset、constructor.*能力标识renewable-lease、future-protocol/);
assert.equal(info.querySelector('.execution-capabilities').querySelectorAll('li')[1].title, 'synthetic');
assert.ok(!info.textContent.includes('节点报告的执行设备'));
assert.equal(nodes[0].querySelectorAll('.resource-state').length, 1);
assert.match(nodes[1].textContent, /尚未获取节点能力/);
const timeline = nodes[0].querySelector('.node-events');
assert.equal(timeline.querySelectorAll('pre').length, 0);
assert.match(timeline.textContent, /接单状态已变更.*停止接单/);
assert.match(timeline.textContent, /节点配置已更新.*CPU 并发配额3.*执行设备gpu-0、gpu-1/);
assert.match(timeline.textContent, /节点连接异常.*安全连接校验失败/);
assert.match(timeline.textContent, /FUTURE_EVENT.*<img src=x onerror=alert\(1\)>.*UNKNOWN_CODE/);
assert.equal(timeline.querySelectorAll('img').length, 0);
assert.equal(info.querySelectorAll('details').length, 0);
assert.equal(nodes[0].querySelectorAll('summary').length, 2);
view.renderResource(resources);
assert.equal(get('node-resources').children[0].querySelector('[data-detail="node-info:node-a"]').open, true);
assert.equal(get('node-resources').children[0].querySelectorAll('summary').length, 2);
assert.equal(get('node-resources').children[0].querySelector('[data-detail="node-info:node-a"]').querySelectorAll('details').length, 0);
assert.equal(get('node-resources').querySelectorAll('button').length, 0);
view.renderResource({nodes: [{...resources.nodes[0], status: 'STALE', events: [],
  capabilities: {...resources.nodes[0].capabilities, references_readable: false, inspection_error: 'ARTIFACT_UNAVAILABLE'}}]});
assert.match(get('node-resources').textContent, /最近一次报告.*共享存储可读性检查：未通过.*产物不可读取/);
assert.match(get('node-resources').textContent, /暂无节点事件/);

const stepA = {id: 'step-a', step_key: 'constructor', registration_order: 1, depends_on: [], status: 'SUCCEEDED', current_attempt_id: attemptA, execution_generation: 3, output_refs: [], execution_node_id: 'node-a', execution_resource_id: 'gpu-0'};
const waiting = {id: 'step-wait', step_key: 'after-prepare', registration_order: 2, depends_on: ['constructor'], status: 'READY', execution_generation: 0, output_refs: [], current_attempt_id: null, execution_node_id: null, execution_resource_id: null, wait_reason: 'WAITING_GPU'};
const task = {id: taskA, pipeline_id: 'managed-v1', status: 'RUNNING', registration_closed: false, wait_reason: 'WAITING_GPU', steps: [stepA, waiting], output_refs: [], recovery_round: 0,
  config_snapshot: {steps: [], runtime_policy: {heartbeat_interval_seconds: 30, lease_ttl_seconds: 300, attempt_hard_timeout_seconds: null}}};
view.task(task, {constructor: [{id: attemptA, node_id: 'node-a', resource_device_id: 'gpu-0', resource_slot_id: 101, worker_id: 'worker-a', attempt_number: 1, fencing_token: 3, status: 'SUCCEEDED', recovery_round: 0, resource_cleanup_state: 'QUARANTINED'}], 'after-prepare': []});
assert.match(get('attempts').textContent, /推理节点 A \/ gpu-0槽位 101Worker worker-a/);
view.inspect('constructor');
assert.match(get('step-inspector').textContent, /执行节点推理节点 A \/ gpu-0/);
view.inspect('after-prepare');
assert.match(get('step-inspector').textContent, /尚未分配执行节点/);
assert.match(get('step-inspector').textContent, /等待原因等待 GPU 执行槽位/);
assert.match(get('task-steps-summary').textContent, /已登记 2 步 \/ 已完成 1 步.*流程未结束/);
view.follow({attempt: attemptA});
assert.equal(view.tab(), 'attempts');
assert.equal(document.activeElement.dataset.attempt, attemptA);
view.follow({step: 'constructor'});
assert.equal(view.tab(), 'flow');
assert.equal(document.activeElement.className, 'step-select');
assert.match(get('step-inspector').textContent, /执行节点推理节点 A \/ gpu-0/);

view.renderQueue({counts: {RUNNING: 1, SUCCEEDED: 0, FAILED: 0}, total: 1, filtered_total: 1, waiting: {}, unconfirmed_count: 0,
  items: [{...task, step_count: 2, completed_steps: 1, cached_steps: 0}]});
assert.equal(get('queue-list').querySelectorAll('progress').length, 0);
const warning = {task_id: taskA, step_key: 'constructor', attempt_id: attemptA, status: 'LOST', resource_cleanup_state: 'CPU_PROCESS_UNCONFIRMED', stop_requested_at: null};
view.renderAttention({unconfirmed_count: 1, unacknowledged_count: 1, unacknowledged_executors: [warning], acknowledged_count: 0, acknowledged_executors: []});
assert.equal(get('resource-warning').textContent, '1 项待核实');
assert.equal(get('resource-warning').hidden, false);
assert.equal(get('resource-attention').querySelector('button').textContent, '确认知悉');
assert.match(get('resource-attention').textContent, /不确认退出或释放资源/);
const knownWarning = {...warning, acknowledged_at: '2026-10-25T00:00:00Z'};
view.renderAttention({unconfirmed_count: 1, unacknowledged_count: 0, unacknowledged_executors: [], acknowledged_count: 1, acknowledged_executors: [knownWarning]});
assert.equal(get('resource-warning').hidden, true);
assert.match(get('resource-attention').textContent, /已知悉记录 · 1 项.*仍未确认退出/);
assert.equal(get('resource-attention').querySelector('button').dataset.restore, 'true');
assert.equal(get('resource-attention').querySelector('a').dataset.executionAttempt, attemptA);
get('resource-attention').querySelector('details').open = true;
view.renderAttention({unconfirmed_count: 2, unacknowledged_count: 1, unacknowledged_executors: [{...warning, attempt_id: attemptB}], acknowledged_count: 1, acknowledged_executors: [knownWarning]});
assert.equal(get('resource-warning').textContent, '1 项待核实');
assert.equal(get('resource-attention').querySelector('details').open, true);
view.renderAttention({unconfirmed_count: 1, unconfirmed_executors: [warning]});
assert.equal(get('resource-warning').hidden, false);
assert.equal(get('resource-attention').querySelectorAll('button').length, 0);
view.renderResource(legacy);
assert.equal(get('node-resources').hidden, false);
assert.equal(get('nodes-status').textContent, '0/0 在线');
assert.equal(get('node-resources').textContent, '暂无配置节点。');
assert.equal(elements.has('resource'), false);
// Product presentation must leave frozen identifiers and measured output intact.
assert.equal(view.stepName(task, 'prepare_demo'), '素材预处理');
assert.equal(view.stepName(task, 'render_demo'), '内容生成');
assert.equal(view.stepName(task, 'constructor'), 'constructor');
assert.equal(view.stepModel(task, 'generate_frames'), '');
const segment = {...stepA, id: 'segment-id', step_key: 'segment_subject', output_refs: []};
const generate = {...stepA, id: 'generate-id', step_key: 'generate_frames', depends_on: ['segment_subject'], output_refs: ['asset-final']};
const animation = {...task, id: taskB, pipeline_id: 'animation-dag-v2', status: 'SUCCEEDED', wait_reason: null,
  created_at: '2026-10-25T00:00:00Z', finished_at: '2026-10-25T00:01:00Z', steps: [segment, generate], output_refs: ['asset-final'],
  config_snapshot: {...task.config_snapshot, request: {input: {kind: 'uploaded', seed: 42}, cache_enabled: false, execution_backend: 'external'},
    input_asset: {id: 'input-1', width: 192, height: 144, media_type: 'image/png'},
    steps: [{key: 'segment_subject'}, {key: 'generate_frames', parameters: {batch_count: 6, frames_per_batch: 8}}]}};
const frozen = JSON.stringify(animation);
view.task(animation, {});
assert.match(get('task-title').textContent, /图生视频工作流/);
assert.equal(get('task-duration').textContent, '1 分 0 秒');
assert.match(get('task-brief').textContent, /image\/png · 192 × 144.*参考模型（演示）SAM 2 \/ Wan2.1-I2V.*随机种子42.*目标帧数48 帧/);
const thumbnail = get('task-brief').querySelector('img');
assert.equal(thumbnail.src, '/api/v1/inputs/input-1/content');
thumbnail.listeners.error();
assert.equal(thumbnail.hidden, true);
assert.equal(get('task-brief').querySelector('p').hidden, false);
assert.equal(get('steps').querySelector('[data-step="generate_frames"]').querySelector('.step-title').textContent, '图生视频生成');
assert.match(get('steps').textContent, /参考模型 · SAM 2.*参考模型 · Wan2.1-I2V/);
const artifact = {id: 'asset-final', step_id: 'generate-id', kind: 'output', availability: 'AVAILABLE',
  metadata: {source: 'mock', preview: {media_type: 'image/gif', width: 192, height: 144}, result_manifest: {frame_count: 48}}};
view.renderArtifacts(animation, [artifact]);
assert.match(get('result').textContent, /结果清单JSON.*预览格式GIF.*预览尺寸192 × 144.*生成帧数48 帧/);
assert.equal(get('result').querySelectorAll('pre').length, 0);
assert.equal(get('result').querySelector('img').src, `/api/v1/tasks/${taskB}/artifacts/asset-final/preview`);
view.eventList(animation, [
  {seq: 1, type: 'ATTEMPT_STARTED', step_id: 'generate-id', attempt_id: attemptA, created_at: '2026-10-25T00:00:00Z', payload: {node_id: 'node-a', worker_id: 'worker-a'}},
  {seq: 2, type: 'CHECKPOINT_COMMITTED', step_id: 'generate-id', payload: {completed_batches: 2, artifact_id: 'checkpoint-1'}},
  {seq: 3, type: 'FUTURE_EVENT', payload: {message: '<script>unsafe</script>'}},
]);
assert.match(get('events').textContent, /生成进度已保存，可用于中断恢复 · 图生视频生成.*已完成批次2.*开始执行 · 图生视频生成/);
assert.match(get('events').textContent, /FUTURE_EVENT.*<script>unsafe<\/script>/);
assert.equal(get('events').querySelectorAll('pre').length, 0);
assert.equal(get('events').querySelectorAll('script').length, 0);
assert.equal(JSON.stringify(animation), frozen);
assert.equal(artifact.metadata.source, 'mock');
view.task(animation, {generate_frames: [{id: attemptA, node_id: 'node-a', resource_device_id: 'cpu', worker_id: 'worker-a', attempt_number: 1, fencing_token: 3, status: 'RUNNING', recovery_round: 0, resource_cleanup_state: 'CPU_ONLY'}]});
assert.match(get('attempts').textContent, /CPU 执行（无 GPU 槽位）/);
assert.ok(!get('attempts').textContent.includes('执行已清理'));
view.renderTaskBrief({...animation, config_snapshot: {request: {input: {kind: 'synthetic', seed: 0}}}});
assert.match(get('task-brief').textContent, /合成素材.*随机种子0/);
assert.equal(get('task-brief').querySelectorAll('img').length, 0);
assert.ok(!get('task-brief').textContent.includes('目标帧数'));
'''
    result = subprocess.run([node, "-e", script], cwd=web, capture_output=True, text=True, encoding="utf-8", timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr


def test_detail_page_and_local_assets_without_database(tmp_path):
    settings = Settings.from_env({"DATABASE_URL": "postgresql://invalid:invalid@127.0.0.1:1/missing", "ARTIFACT_ROOT": str(tmp_path)})
    with TestClient(create_app(settings)) as client:
        for path in ("/", f"/tasks/{uuid4()}"):
            page = client.get(path)
            assert page.status_code == 200
            assert 'id="task-content"' in page.text
            assert '/assets/app.js' in page.text
            assert 'AIGC 任务调度与监控平台' in page.text
            assert '模拟演示环境' in page.text
            assert '模型推理与算力环境为模拟实现' in page.text
        assert client.get('/assets/app.css').status_code == 200
        assert client.get('/assets/app.js').status_code == 200
        assert client.get('/tasks/not-a-uuid').status_code == 422
        assert client.get('/assets/../api.py').status_code == 404
        assert client.get('/api/v1/tasks').status_code == 503
        assert client.get('/api/v1/monitor').status_code == 503
