const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

function renderer() {
  const context = {window:{}, module:{exports:{}}};
  vm.runInNewContext(fs.readFileSync('frontend/investigations.js','utf8'),context);
  return context.module.exports.resultHtml;
}

test('specific string advice reaches both screen and report without HTML injection', () => {
  const render=renderer();
  const data={status:'Analysis complete', results:[{name:'<script>x</script>', files:['x'], quality:{}, time_basis:'relative',
    analysis:{metrics:{event_count:2}, health:{grade:'C',status:'watch'}, findings:{recommendations:['Check heap pressure <now>']}}}],
    correlation:{summary:'Unavailable',matched_minutes:0}};
  for(const charts of [false,true]) {
    const html=render(data,charts);
    assert.match(html,/Check heap pressure &lt;now&gt;/);
    assert.doesNotMatch(html,/<script>x/);
  }
});
