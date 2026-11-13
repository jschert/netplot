"use strict";

const assert = require("node:assert/strict");

// Run with a browser harness supplying {page, tab}, on an isolated dashboard
// with a running multi-hop trace at a 1-second interval. Uses native input,
// not scripted Selection ranges, so it also exercises mid-drag refreshes.
module.exports = async function checkHopSelection({ page, tab }) {
  const hop = await page.evaluate(() => {
    const primary = document.querySelector(".timeline-close[hidden]").closest(".timeline-panel").dataset.hop;
    const row = Array.from(document.querySelectorAll("#hop-table-body tr[data-hop]"))
      .reverse().find(row => row.dataset.hop !== primary && row.querySelector(".cell-ip").textContent !== "*");
    return row ? row.dataset.hop : null;
  });
  assert.notEqual(hop, null, "Supply a trace with a non-primary, responding hop");
  const row = `#hop-table-body tr[data-hop="${hop}"]`;
  const panel = `.timeline-panel[data-hop="${hop}"]`;

  async function selectAcrossRounds(cell) {
    const selector = row + " " + cell;
    const before = await page.evaluate(selector => {
      const cell = document.querySelector(selector);
      const range = document.createRange();
      range.selectNodeContents(cell);
      const rect = range.getBoundingClientRect();
      return {
        text: cell.textContent,
        count: Number(cell.parentNode.querySelector(".cell-count").textContent),
        x: rect.x, y: rect.y, width: rect.width, height: rect.height,
      };
    }, selector);
    await page.mouse.move(before.x + 1, before.y + before.height / 2);
    await page.mouse.down();
    try {
      await page.mouse.move(before.x + before.width - 1, before.y + before.height / 2, { steps: 5 });
      assert.equal(await page.evaluate(() => getSelection().toString()), before.text);
      await page.waitForFunction((row, count) =>
        Number(document.querySelector(row + " .cell-count").textContent) >= count + 2,
      { timeout: 15000 }, row, before.count);
      assert.equal(await page.evaluate(() => getSelection().toString()), before.text,
        "Live rounds must not redirect a held drag into preceding rows");
    } finally {
      await page.mouse.up();
    }
    const count = await page.evaluate(row => Number(document.querySelector(row + " .cell-count").textContent), row);
    await page.waitForFunction((row, count) =>
      Number(document.querySelector(row + " .cell-count").textContent) > count,
    { timeout: 15000 }, row, count);
    assert.equal(await page.evaluate(() => getSelection().toString()), before.text,
      "Selection must survive updates after mouseup too");
    await tab.clipboardCopy();
    assert.equal((await tab.clipboardRead()).text, before.text,
      "Native copy must contain only the selected cell text");
    return before.text;
  }

  const ip = await selectAcrossRounds(".cell-ip");
  const dns = await selectAcrossRounds(".cell-dns");
  const panelCount = await tab.count(".timeline-panel");
  await tab.dblclick(row + " .cell-ip");
  assert.equal(await tab.count(".timeline-panel"), panelCount, "IP double-click must only select text");
  await tab.dblclick(row + " .cell-dns");
  assert.equal(await tab.count(".timeline-panel"), panelCount, "DNS double-click must only select text");
  assert.equal(await tab.count(panel), 0);
  await tab.dblclick(row + " .cell-hop");
  await page.waitForSelector(panel);
  await tab.dblclick(row + " .cell-hop");
  await page.waitForFunction(selector => !document.querySelector(selector), {}, panel);
  assert.equal(await tab.count(".timeline-panel"), panelCount, "Hop double-click must still toggle its timeline");
  return { ip, dns, selectionSurvivesLiveRounds: true, nativeCopy: true, timelineGestures: true };
};
