import { consume } from "../esnext/index.js";
import { Dataset } from "@opennetwork/rdf-dataset"
import {DefaultDataFactory} from "@opennetwork/rdf-data-model";
import htm from "htm/preact/index.js"

async function *thing() {
  yield 1
  yield ""
  yield new Date()
  yield {
    a: 1
  }
  yield [1]
  yield 1n
  yield -1n
  yield -0n
  yield 0n
  yield htm.html`<${thing} href="/">Hello</>`
}

async function run() {

  const dataset = new Dataset()
  const graph = DefaultDataFactory.blankNode(".")
  const source = consume(thing(), {
    literalQuad: {
      subject: {
        termType: "NamedNode",
        value: "https://example.com"
      },
      graph: graph
    },
    profileQuad: {
      graph: DefaultDataFactory.blankNode("profile")
    }
  })
  await dataset.import(source)

  console.log(JSON.stringify(dataset.toArray(), undefined, "  "))
}

run()
  .then(() => {
    process.exit(0)
  })
  .catch(error => {
    console.error(error)
    process.exit(1)
  })
