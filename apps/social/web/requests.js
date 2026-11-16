function fetchRequestPage(recheck) {
  if (recheck) return api('sharing-requests/recheck', {});
  const params = new URLSearchParams();
  if (state.requestsCursor) params.set('cursor', JSON.stringify(state.requestsCursor));
  return api(`sharing-requests?${params}`);
}

function openRequestPage(cursor, recheck = false) {
  state.requestsCursor = cursor;
  state.selected = null;
  return refresh(recheck);
}

function renderRequestList() {
  const {requests, nextCursor, importing} = state.requests;
  $('heading').textContent = 'Requests';
  const actions = [button('Refresh', () => openRequestPage(null, true), 'quiet')];
  if (state.requestsCursor) actions.push(button('First page', () => openRequestPage(null), 'quiet'));
  if (importing) actions.push(button('Load more saved requests', () => openRequestPage(null), 'quiet'));
  else if (nextCursor) actions.push(button('Next page', () => openRequestPage(nextCursor), 'quiet'));
  $('actions').replaceChildren(...actions);
  if (!requests.some(({requestId}) => requestId === state.selected)) state.selected = requests[0]?.requestId ?? null;
  const names = new Map(state.home?.databases.map((row) => [row.id, nameOf(row)]));
  $('list').replaceChildren(...requests.map(({requestId, appId, databaseId}) => {
    const entry = button(appId, () => {
      state.selected = requestId;
      renderList();
      return renderSelection();
    }, 'entry');
    entry.append(element('span', names.get(databaseId) || databaseId));
    if (requestId === state.selected) entry.setAttribute('aria-current', 'true');
    return entry;
  }));
  if (!requests.length) {
    const message = importing ? 'Importing saved requests.'
      : state.requestsCursor ? 'No requests awaiting approval on this page.' : 'No requests awaiting approval.';
    $('list').append(element('p', message, 'muted'));
  }
}
