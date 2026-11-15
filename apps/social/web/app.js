const $ = (id) => document.getElementById(id);
const session = new URLSearchParams(location.hash.slice(1)).get('session');
const state = {tab: 'people', selected: null, home: null, request: 0};

function element(tag, text, className) {
  const node = document.createElement(tag);
  if (text != null) node.textContent = text;
  if (className) node.className = className;
  return node;
}

function button(text, action, className = '') {
  const item = element('button', text, className);
  item.type = 'button';
  item.addEventListener('click', () => void Promise.resolve().then(action).catch(report));
  return item;
}

async function api(path, body) {
  if (!session) throw new Error('Open Social using the address printed by the launcher.');
  const response = await fetch(`/api/${path}`, {
    method: body ? 'POST' : 'GET', headers: {'X-Social-Session': session, 'Content-Type': 'application/json'},
    body: body ? JSON.stringify(body) : undefined,
  });
  const result = await response.json();
  if (!response.ok) throw new Error(result.error || 'Social could not complete the request.');
  return result;
}

function report(error) {
  $('notice').textContent = error.message || error;
  $('notice').hidden = false;
}

function nameOf(row) { return row.name || row.id || row.databaseId; }
function specOf(row) {
  if (row.sharingMode === 'whole_database') return 'Whole database';
  return row.spec ? `${row.spec.name} v${row.spec.version}` : 'Spec unavailable';
}
function isOwned(row) { return row.kind === 'shareable'; }
function accessOf(row) {
  if (isOwned(row)) return 'Owner';
  const access = row.selfAccess;
  if (!access || access.state === 'unknown') return 'Cloud access unconfirmed';
  const roles = {read: 'Reader', submit: 'Contributor'};
  const labels = {active: roles[access.role] || 'Access active', revoked: 'Revoked', absent: 'No grant'};
  return `${access.stale ? 'Last confirmed · ' : ''}${labels[access.state] || 'Unconfirmed'}`;
}

async function refresh() {
  const ticket = ++state.request;
  $('notice').hidden = true;
  const home = await api('home');
  if (ticket !== state.request) return;
  state.home = home;
  renderList();
  await renderSelection();
}

function setTab(tab, selected = null) {
  state.tab = tab;
  state.selected = selected;
  if (!state.home) return refresh();
  renderList();
  return renderSelection();
}

function renderList() {
  const people = state.tab === 'people';
  $('heading').textContent = people ? 'People' : 'Databases';
  for (const tab of ['people', 'databases']) {
    if (tab === state.tab) $(tab).setAttribute('aria-current', 'page');
    else $(tab).removeAttribute('aria-current');
  }
  $('actions').replaceChildren(...(people ? [button('Add contact', remember)] : [
    button('Accept invitation', accept, 'quiet'), button('New database', create),
  ]), button('Refresh', refresh, 'quiet'));
  const rows = state.home[state.tab];
  if (!rows.some((row) => (people ? row.nodeKey : row.id) === state.selected)) {
    state.selected = rows.length ? (people ? rows[0].nodeKey : rows[0].id) : null;
  }
  $('list').replaceChildren(...rows.map((row) => {
    const key = people ? row.nodeKey : row.id;
    const entry = button(nameOf(row), () => setTab(state.tab, key), 'entry');
    if (!people) entry.append(element('span', `${specOf(row)} · ${accessOf(row)}`));
    if (key === state.selected) entry.setAttribute('aria-current', 'true');
    return entry;
  }));
  if (!rows.length) $('list').append(element('p', people ? 'No saved contacts.' : 'No shared databases.', 'muted'));
}

async function renderSelection() {
  const ticket = ++state.request;
  const {tab, selected} = state;
  const panel = $('detail');
  panel.replaceChildren();
  if (!selected) return;
  panel.append(element('p', 'Loading…', 'muted'));
  const people = tab === 'people';
  const query = new URLSearchParams({[people ? 'nodeKey' : 'databaseId']: selected});
  const result = await api(`${people ? 'person' : 'database'}?${query}`);
  if (ticket !== state.request) return;
  panel.replaceChildren();
  if (people) showPerson(panel, result);
  else showDatabase(panel, result);
}

function section(panel, title, rows, empty) {
  panel.append(element('h3', title, 'section-title'));
  if (!rows.length) panel.append(element('p', empty, 'muted'));
  for (const row of rows) panel.append(row);
}

function showPerson(panel, result) {
  panel.append(element('h2', result.person.name), element('p', result.person.nodeKey, 'identity muted'));
  const rows = (items, incoming) => items.map((item) => {
    const row = element('div', null, 'row');
    row.append(button(nameOf(item), () => setTab('databases', item.id || item.databaseId), 'quiet'),
      element('span', incoming ? accessOf(item) : item.status || `${item.role} · ${item.state}`));
    return row;
  });
  section(panel, 'You share', rows(result.outgoing, false), 'No outgoing shares.');
  section(panel, 'Shared with you', rows(result.incoming, true), 'No incoming shares.');
}

function showDatabase(panel, result) {
  const {database, sharing, pendingEdit, circles, schemas} = result;
  panel.append(element('h2', nameOf(database)), element('p', `${specOf(database)} · ${accessOf(database)}`, 'muted'));
  if (!sharing) {
    panel.append(element('p', database.state === 'ready' ? 'Downloaded records available' : 'Waiting for initial sync', 'status'));
    if (database.publisher) panel.append(element('p', `Publisher ${database.publisher}`, 'identity muted'));
    if (database.selfAccess?.confirmedAtMs) panel.append(element('p',
      `Cloud access confirmed ${new Date(Number(database.selfAccess.confirmedAtMs)).toLocaleString()}`, 'muted'));
    if (database.sharingMode === 'whole_database' && database.selfAccess?.state === 'active') {
      const scope = database.selfAccess.writeSchemas;
      panel.append(element('p', scope === 'all' ? 'Can read the whole database and write all schemas.'
        : scope?.length ? 'Can read the whole database and write selected schemas.'
          : 'Can read the whole database. The publisher granted no schema writes.', 'status'));
    }
    return;
  }
  if (pendingEdit) {
    panel.append(element('p', `Pending change based on revision ${pendingEdit.expectedRevision}.`, 'status'),
      button('Retry pending change', async () => { await api('retry', {databaseId: database.id}); await refresh(); }, 'quiet'));
  }
  if (sharing.status) panel.append(element('p', `Roster ${sharing.status}.`, 'status'));
  else panel.append(element('p', `Confirmed revision ${sharing.revision}`, 'status'));
  if (!sharing.status && circles) {
    panel.append(element('p', 'Members can read the whole database. Choose the schemas each circle can write to.', 'muted'));
    section(panel, 'Circles', circles.map((circle) => {
      const row = element('div', null, 'row');
      row.append(element('span', `${circle.name} · ${circle.members.length} member(s)`),
        element('span', circle.writeSchemas === 'all' ? 'All schemas' : circle.writeSchemas.join(', ') || 'Read only'));
      return row;
    }), 'No circles share this database.');
    if (!pendingEdit) panel.append(button('Edit circles', () => editCircles(database, sharing, circles, schemas), 'quiet'));
  }
  if (!sharing.status && !circles) panel.append(button('Invite reader', () => invite(database), 'quiet'));
  const members = (sharing.members || []).map((member) => {
    const row = element('div', null, 'row');
    const label = element('div', member.name || member.nodeKey, 'identity');
    const writes = member.writeSchemas;
    const names = writes === 'all' ? 'All schemas' : writes?.map((id) =>
      Object.keys(schemas || {}).find((name) => schemas[name] === id) || id).join(', ');
    label.append(element('span', `${names || (member.role === 'read' ? 'Read only' : 'Contributor')} · ${member.state}`, 'muted'));
    row.append(label);
    const actions = element('div', null, 'row-actions');
    if (circles && member.state === 'active' && member.name) {
      actions.append(button('Seal invitation', async () => showReceipt(await api('circle-invitation', {
        databaseId: database.id, nodeKey: member.nodeKey,
      })), 'quiet'));
    }
    if (member.state === 'active') actions.append(button('Revoke', () => revoke(database, member), 'quiet'));
    row.append(actions);
    return row;
  });
  if (!sharing.status) section(panel, 'People with access', members, 'Nobody has been invited.');
}

function dialog(title, label, fields, submit) {
  const form = $('form');
  const dialog = $('form-dialog');
  $('form-title').textContent = title;
  $('form-error').hidden = true;
  $('submit').textContent = label;
  $('submit').disabled = false;
  $('fields').replaceChildren(...fields.map(({name, label, type, options, optional, value}) => {
    const wrapper = element('label', label);
    const control = element(type === 'textarea' ? 'textarea' : options ? 'select' : 'input');
    control.name = name;
    control.required = !optional && !['checkbox', 'multiselect'].includes(type);
    if (type === 'checkbox') control.type = 'checkbox';
    if (type === 'multiselect') control.multiple = true;
    for (const option of options || []) {
      const node = element('option', option.name);
      node.value = option.nodeKey;
      if (type === 'multiselect') node.selected = value?.includes(option.nodeKey) ?? false;
      control.append(node);
    }
    if (value !== undefined && type !== 'multiselect') control.value = value;
    wrapper.append(control);
    return wrapper;
  }));
  form.onsubmit = async (event) => {
    event.preventDefault();
    $('submit').disabled = true;
    $('form-error').hidden = true;
    try {
      const data = new FormData(form);
      const input = Object.fromEntries(fields.map(({name, type}) => [
        name, type === 'multiselect' ? data.getAll(name) : type === 'checkbox' ? data.has(name) : data.get(name),
      ]));
      const value = await submit(input);
      dialog.close();
      if (typeof value === 'function') {
        value();
        return;
      }
      try { await refresh(); } catch (error) { report(error); }
      if (value?.invitation) showReceipt(value);
    } catch (error) {
      if (!dialog.open) { report(error); return; }
      $('form-error').textContent = error.message;
      $('form-error').hidden = false;
    } finally { $('submit').disabled = false; }
  };
  dialog.showModal();
}

function remember() {
  dialog('Add contact', 'Save contact', [{name: 'name', label: 'Name'}, {name: 'nodeKey', label: 'Verified node key'},
    {name: 'contactCode', label: 'LastDB contact code', type: 'textarea'}], async (data) => {
    const person = await api('remember', data);
    state.selected = person.nodeKey;
  });
}

function create() {
  const options = [
    {name: 'Social albums and photos', nodeKey: 'social'},
    {name: 'Whole database with circle write access', nodeKey: 'whole_database'},
    {name: 'Shared albums (version 1)', nodeKey: 'shared-albums'},
  ];
  dialog('New database', 'Create database', [
    {name: 'name', label: 'Name'},
    {name: 'sharingMode', label: 'Database setup', options},
  ], async (data) => {
    if (data.sharingMode === 'shared-albums') {
      const setup = await api('setup-database', {setupName: 'shared-albums', localName: data.name});
      state.selected = setup.databaseId;
    } else {
      const database = await api('create', data);
      state.selected = database.id;
    }
  });
}

function editCircles(database, sharing, circles, schemas) {
  const options = [{name: 'New circle', nodeKey: ''}, ...circles.map((circle) => ({name: circle.name, nodeKey: circle.name}))];
  dialog('Circle settings', 'Continue', [{name: 'circle', label: 'Circle', options, optional: true}],
    ({circle}) => () => editCircle(database, sharing, circles, schemas, circle));
}

function editCircle(database, sharing, circles, schemas, name) {
  const current = circles.find((circle) => circle.name === name);
  const schemaOptions = [{name: 'All schemas, including future schemas', nodeKey: 'all'},
    ...Object.keys(schemas).sort().map((key) => ({name: key, nodeKey: key}))];
  const revoked = sharing.members.filter((member) => member.state === 'revoked');
  const fields = [
    {name: 'name', label: 'Circle name', value: name},
    {name: 'members', label: 'Members', type: 'multiselect', options: state.home.people, value: current?.members},
    {name: 'writeSchemas', label: 'Writable schemas', type: 'multiselect', options: schemaOptions,
      value: current?.writeSchemas === 'all' ? ['all'] : current?.writeSchemas},
  ];
  if (revoked.length) fields.push({name: 'restore', label: 'Explicitly restore selected members',
    type: 'multiselect', options: revoked.map((member) => ({name: member.name || member.nodeKey, nodeKey: member.nodeKey}))});
  if (current) fields.push({name: 'remove', label: 'Remove this circle from the database', type: 'checkbox'});
  dialog('Circle access', 'Apply circle grants', fields, async (data) => {
    const next = circles.filter((circle) => circle.name !== name);
    if (!data.remove) next.push({name: data.name, members: data.members,
      writeSchemas: data.writeSchemas.includes('all') ? 'all' : data.writeSchemas});
    await api('circles', {databaseId: database.id, expectedRevision: sharing.revision, circles: next,
      restore: data.restore ?? []});
  });
}

function accept() {
  dialog('Accept invitation', 'Open database', [{name: 'publisher', label: 'Verified publisher node key'},
    {name: 'receipt', label: 'Invitation receipt', type: 'textarea'}, {name: 'name', label: 'Local name', optional: true}], async (data) => {
    const database = await api('accept', {...data, receipt: JSON.parse(data.receipt), name: data.name || null});
    state.selected = database.id;
  });
}

function invite(database) {
  if (!state.home.people.length) return remember();
  dialog('Invite reader', 'Create invitation', [{name: 'nodeKey', label: 'Person', options: state.home.people}],
    (data) => api('invite', {...data, databaseId: database.id}));
}

function revoke(database, member) {
  dialog(`Revoke ${member.name || 'this person'}?`, 'Revoke access', [],
    () => api('revoke', {nodeKey: member.nodeKey, databaseId: database.id}));
}

function showReceipt(receipt) {
  const panel = element('div', null, 'invitation');
  panel.append(element('h3', 'Sealed invitation'));
  const input = element('textarea');
  input.value = JSON.stringify(receipt);
  input.readOnly = true;
  input.setAttribute('aria-label', 'Sealed invitation receipt');
  panel.append(input, button('Select invitation', () => input.select(), 'quiet'));
  $('detail').append(panel);
}

async function showPicker() {
  $('brand').removeAttribute('href');
  document.querySelector('nav').hidden = true;
  $('heading').textContent = 'Choose a database';
  $('detail').hidden = true;
  $('list').parentElement.classList.add('picker');
  const {intent, databases} = await api('choices');
  const complete = async (databaseId) => {
    const controls = [...document.querySelectorAll('main button')];
    controls.forEach((control) => { control.disabled = true; });
    try {
      await api('choose', {databaseId});
      $('list').replaceChildren(element('p', 'You can close this window.'));
      $('actions').replaceChildren();
    } catch (error) {
      controls.forEach((control) => { control.disabled = false; });
      throw error;
    }
  };
  $('actions').append(button('Cancel', () => complete(null), 'quiet'));
  $('list').append(element('p', intent === 'write' ? 'Allow the app to read and write.' : 'Allow the app to read.', 'status'));
  if (!databases.length) $('list').append(element('p', 'No matching databases are available.', 'muted'));
  for (const row of databases) {
    const choice = button(nameOf(row), () => complete(row.databaseId), 'entry');
    choice.append(element('span', `${specOf(row)} · ${accessOf(row)}`));
    $('list').append(choice);
  }
}

function showSocial(databaseId) {
  for (const tab of ['people', 'databases']) $(tab).addEventListener('click', () => setTab(tab).catch(report));
  $('cancel').addEventListener('click', () => $('form-dialog').close());
  $('brand').addEventListener('click', (event) => { event.preventDefault(); setTab('people').catch(report); });
  if (databaseId) { state.tab = 'databases'; state.selected = databaseId; }
  return refresh();
}

const view = new URLSearchParams(location.search);
(view.has('choose') ? showPicker() : showSocial(view.get('databaseId'))).catch(report);
