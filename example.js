import assert from 'assert'
import {connect} from './'

const db = connect('postgres://localhost/test')

const Person = db.define('person', {
  first_name: {type: 'text', limit: 50},
  last_name : {type: 'text', limit: 50},
  gender    : {type: 'enum', values: ['male', 'female']},
  birth     : {type: 'timestamp'},
  pay_rate  : {type: 'smallint'},
  home      : {type: 'point'}
})

const Shift = db.define('shift', {
  start      : {type: 'timestamp'},
  end        : {type: 'timestamp'},
  title      : {type: 'text', limit: 50},
  description: {type: 'text'},
  workers    : {type: [Person]},
  employer   : {type: Person}
})

const Review = db.define('review', {
  rating : {type: 'smallint'},
  message: {type: 'text'},
  to     : {type: Person},
  from   : {type: Person},
  shift  : {type: Shift}
})

const main = async () => {
  const jake = await Person.create({
    first_name: 'Jake',
    last_name : 'Rosoman',
    gender: 'male',
    birth: new Date(1991, 4, 29),
    pay_rate: 20,
    home: [175.224215, -37.821275],
  })

  const boss = await Person.create({
    first_name: 'Al',
    last_name : 'Capone',
    gender: 'male',
    birth: new Date(1965, 2, 23),
    pay_rate: 20,
    home: [175.223215, -37.921275],
  })

  const goatshedjob = await Shift.create({
    start: new Date(2015, 10, 15, 9),
    end: new Date(2015, 10, 15, 17),
    title: 'Goat shed extension',
    description: 'Add one segment and install extra clear lights throughout',
    employer: boss,
    workers: [jake],
  })

  const review = await Review.create({
    rating: 5,
    message: 'Perfect, couldn\'t ask for more',
    to: jake,
    from: boss,
    shift: goatshedjob
  })

  const jakes = await Person.find({first_name: 'Jake'})
  assert(jakes[0].id == jake.id)
  assert((await review.to).id == jake.id)
  const shift = await review.shift
  assert(shift.id == goatshedjob.id)
  console.log('all passed')
}

main().catch(e => console.error(e.stack))
