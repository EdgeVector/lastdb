function renderReusableList() {
  const {circles, pending} = state.directory;
  $('heading').textContent = 'Circles';
  for (const tab of ['people', 'circles', 'databases']) {
    if (tab === 'circles') $(tab).setAttribute('aria-current', 'page');
    else $(tab).removeAttribute('aria-current');
  }
  const action = pending.length ? button('Review unfinished changes', () => previewReusable({}))
    : button('New circle', () => editReusable());
  $('actions').replaceChildren(action, button('Refresh', refresh, 'quiet'));
  if (!circles.some((circle) => circle.id === state.selected)) state.selected = circles[0]?.id ?? null;
  $('list').replaceChildren(...circles.map((circle) => {
    const entry = button(circle.name, () => { state.selected = circle.id; renderReusableList();
      return renderSelection(); }, 'entry');
    if (circle.id === state.selected) entry.setAttribute('aria-current', 'true');
    return entry;
  }));
  if (!circles.length) $('list').append(element('p', 'No reusable circles.', 'muted'));
}

function renderReusableCircle(panel, selected) {
  const {circles, databases, pending} = state.directory;
  const circle = circles.find((item) => item.id === selected);
  if (!circle) return;
  const name = (key) => state.home.people.find((person) => person.nodeKey === key)?.name ?? key;
  panel.append(element('h2', circle.name));
  if (pending.length) panel.append(element('p', `${pending.length} database update(s) need review.`, 'status'));
  const people = circle.members.map((key) => element('div', name(key), 'row'));
  section(panel, 'People', people, 'No members.');
  const links = Object.entries(circle.links).map(([id, scope]) => {
    const row = element('div', null, 'row');
    const database = databases.find((item) => item.id === id);
    row.append(button(database?.name || id, () => setTab('databases', id), 'quiet'),
      element('span', scopeOf(scope)));
    return row;
  });
  section(panel, 'Databases', links, 'No linked databases.');
  if (!pending.length) panel.append(button('Edit circle', () => editReusable(circle)),
    button('Remove circle', () => previewReusable({circle, remove: true}), 'quiet'));
}

function editReusable(circle) {
  const {databases} = state.directory;
  const fields = [{name: 'name', label: 'Circle name', value: circle?.name},
    {name: 'members', label: 'Members', type: 'multiselect', options: state.home.people, value: circle?.members}];
  const known = new Set(databases.map((item) => item.id));
  const links = Object.entries(circle?.links ?? {}).filter(([id]) => !known.has(id));
  const available = [...databases, ...links.map(([id]) => ({id, name: id, schemas: null}))];
  for (const database of available) {
    const current = circle?.links[database.id];
    const names = new Set([...(database.schemas ?? []), ...(Array.isArray(current) ? current : [])]);
    fields.push({name: database.id, label: database.name || database.id, type: 'multiselect', options: [
      {name: 'Read whole database', nodeKey: 'read'}, {name: 'Write all schemas', nodeKey: 'all'},
      ...[...names].sort().map((name) => ({name: `Write ${name}`, nodeKey: `schema:${name}`})),
    ], value: current === 'all' ? ['all'] : current ? ['read', ...current.map((name) => `schema:${name}`)] : []});
  }
  dialog(circle ? 'Edit circle' : 'New circle', 'Preview changes', fields, (data) => {
    const selected = {};
    for (const {id} of available) {
      if (data[id].length) selected[id] = data[id].includes('all') ? 'all'
        : data[id].filter((value) => value.startsWith('schema:')).map((value) => value.slice(7));
    }
    const next = {id: circle?.id ?? crypto.randomUUID(), name: data.name, members: data.members, links: selected};
    return () => previewReusable({circle: next});
  });
}

function scopeOf(scope) {
  if (scope === 'all') return 'Read all, write all schemas';
  const names = scope?.names ?? scope ?? [];
  return names.length ? `Read all, write ${names.join(', ')}` : 'Read only';
}

function describeGrant(grant) {
  return !grant || grant.state === 'revoked' ? 'No access' : scopeOf(grant.writeSchemas);
}

async function previewReusable(edit) {
  const {token, databases} = await api('circles/preview', edit);
  const review = grantReviewFields(databases);
  dialog('Review circle changes', review.fields.length ? 'Preview resolutions' : 'Apply changes', review.fields, async (data) => {
    if (review.fields.length) return () => previewReusable({...edit, review: review.choices(data, edit.review)});
    const result = await api('circles/apply', {...edit, token});
    return async () => {
      await refresh();
      if (result.pending.length) report(new Error('Some database changes need review. Open the unfinished changes to continue.'));
    };
  });
  renderGrantImpact(databases);
  if (databases.some((row) => row.error && !row.conflicts) && edit.circle && !review.fields.length) {
    $('submit').disabled = true;
  }
}

function contactName(key) {
  return state.home.people.find((person) => person.nodeKey === key)?.name ?? key;
}

function grantReviewFields(databases) {
  const conflicts = databases.flatMap((row) => (row.conflicts ?? []).map((conflict) => ({
    ...conflict, databaseId: row.databaseId, name: row.name,
    field: `${row.databaseId}:${conflict.nodeKey}`,
  })));
  return {
    fields: conflicts.map((row) => ({
      name: row.field,
      label: `${row.name} · ${contactName(row.nodeKey)} · current grant ${describeGrant(row.current)}`,
      options: [{nodeKey: '', name: 'Choose a resolution'},
        {nodeKey: 'keepDirect', name: 'Keep the outside grant as a direct grant'},
        {nodeKey: 'useSocial', name: 'Apply the Social grants'}],
    })),
    choices(values, existing = {}) {
      const result = {...existing};
      for (const row of conflicts) {
        result[row.databaseId] = {...result[row.databaseId], [row.nodeKey]: values[row.field]};
      }
      return result;
    },
  };
}

function renderGrantImpact(databases) {
  const fields = $('fields');
  for (const row of databases) {
    fields.append(element('h3', row.name));
    if (row.error) {
      fields.append(element('p', row.error));
      continue;
    }
    if (!row.changes.length) fields.append(element('p', 'Access stays the same.', 'muted'));
    for (const {nodeKey, before, after} of row.changes) {
      fields.append(element('p', `${contactName(nodeKey)} · ${describeGrant(before)} → ${describeGrant(after)}`));
    }
  }
}
